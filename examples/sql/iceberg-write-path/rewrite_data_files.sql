-- Compaction: rewrite small files into fewer, larger files.
-- Run as a **scheduled** maintenance job, not in every ETL pass.

-- CALL prod.system.rewrite_data_files(
--   table => 'my_catalog.mydb.my_iceberg_table',
--   options => map('target-file-size-bytes', '134217728')
-- );
--
-- (Procedure name and arguments vary by Iceberg/Spark version — check your runtime docs.)
