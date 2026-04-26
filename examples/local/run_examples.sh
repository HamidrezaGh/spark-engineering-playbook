#!/usr/bin/env bash
#
# Run a few SQL examples against the local sample data using a local Spark.
# This is a teaching harness, not a benchmark. The goal is for the reader to
# inspect physical plans, the Spark UI, and runtime metrics on tiny inputs.
#
# Requirements:
#   - Apache Spark 3.4+ installed locally (binaries on PATH).
#   - `spark-sql` available, or PySpark installed in a venv.
#   - The sample data files in ./data/ (already in the repo).
#
# Usage:
#   ./run_examples.sh                 # run all
#   ./run_examples.sh sql             # SQL examples only (uses spark-sql)
#   ./run_examples.sh pyspark         # PySpark inspection scripts only
#
# After each run, open http://localhost:4040 (Spark UI) while the job is
# still alive to inspect the SQL tab and Stages tab. For SQL examples, also
# read the EXPLAIN FORMATTED output printed at the start of each query.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
DATA_DIR="${HERE}/data"
SQL_DIR="${REPO_ROOT}/examples/sql"
PYSPARK_DIR="${REPO_ROOT}/examples/pyspark"

MODE="${1:-all}"

print_header() {
    echo
    echo "=================================================================="
    echo "  $*"
    echo "=================================================================="
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "Required command not found: $1"
        echo "Install Apache Spark and ensure '$1' is on your PATH."
        exit 1
    }
}

run_sql_examples() {
    require_cmd spark-sql

    print_header "Loading sample data into a temporary local Spark session"

    local tmp_init="${HERE}/.init.sql"
    cat > "${tmp_init}" <<EOF
-- Create temp views over the local CSVs. These views replace the
-- 'events' and 'customers' tables referenced in the SQL examples.

CREATE OR REPLACE TEMPORARY VIEW events
USING csv
OPTIONS (
    path '${DATA_DIR}/events_sample.csv',
    header 'true',
    inferSchema 'true'
);

CREATE OR REPLACE TEMPORARY VIEW customers
USING csv
OPTIONS (
    path '${DATA_DIR}/customers_sample.csv',
    header 'true',
    inferSchema 'true'
);

-- 'orders' is aliased to 'events' for the join examples that use 'orders'.
-- The schema is close enough for plan-reading purposes; this is not a
-- production-equivalent dataset.
CREATE OR REPLACE TEMPORARY VIEW orders AS
SELECT
    event_id   AS order_id,
    event_date AS order_date,
    customer_id,
    amount     AS order_amount
FROM events;

CREATE OR REPLACE TEMPORARY VIEW dim_country AS
SELECT DISTINCT
    country_id,
    country_id AS country_name
FROM customers
WHERE country_id IS NOT NULL;
EOF

    local sql_files=(
        "${SQL_DIR}/01-explain-shuffle.sql"
        "${SQL_DIR}/02-broadcast-vs-sort-merge-join.sql"
        "${SQL_DIR}/03-skew-detection.sql"
        "${SQL_DIR}/04-window-vs-groupby.sql"
        "${SQL_DIR}/05-partition-pruning.sql"
    )

    for sql in "${sql_files[@]}"; do
        if [[ -f "${sql}" ]]; then
            print_header "Running ${sql##*/}"
            spark-sql \
                --conf spark.sql.shuffle.partitions=8 \
                --conf spark.sql.adaptive.enabled=true \
                --conf spark.ui.enabled=true \
                -f "${tmp_init}" \
                -f "${sql}" || echo "Continuing after non-fatal error in ${sql##*/}"
        fi
    done

    rm -f "${tmp_init}"
}

run_pyspark_examples() {
    require_cmd python3
    python3 -c "import pyspark" 2>/dev/null || {
        echo "PySpark not available. Install with: pip install pyspark"
        exit 1
    }

    print_header "Running PySpark inspection scripts on local sample data"

    if [[ -f "${PYSPARK_DIR}/inspect_partitions.py" ]]; then
        print_header "inspect_partitions.py"
        python3 "${PYSPARK_DIR}/inspect_partitions.py" \
            --input "${DATA_DIR}/events_sample.csv" \
            --format csv \
            --header || true
    fi

    if [[ -f "${PYSPARK_DIR}/skew_detector.py" ]]; then
        print_header "skew_detector.py"
        python3 "${PYSPARK_DIR}/skew_detector.py" \
            --input "${DATA_DIR}/events_sample.csv" \
            --format csv \
            --header \
            --key customer_id \
            --top-n 10 || true
    fi
}

case "${MODE}" in
    sql)
        run_sql_examples
        ;;
    pyspark)
        run_pyspark_examples
        ;;
    all)
        run_sql_examples
        run_pyspark_examples
        ;;
    *)
        echo "Usage: $0 [sql|pyspark|all]"
        exit 2
        ;;
esac

echo
echo "Done. While Spark was running you could open http://localhost:4040"
echo "to inspect the Spark UI. After the driver exits the UI is gone unless"
echo "spark.eventLog.enabled is set."
