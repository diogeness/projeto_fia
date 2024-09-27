-- rodar os dois scrips abaixo no clickhouse para criar as views

CREATE OR REPLACE VIEW sp_trans AS
SELECT
    *
FROM s3(
    'http://minio:9000/trusted/sptrans/*/*/*/*/*',
    '${MINIO_ACCESS_KEY}',
    '${MINIO_SECRET_KEY}',
    'Parquet');
   
   
CREATE OR REPLACE VIEW sp_trans_live AS
SELECT
    *, 1 qtd
FROM s3(
    'http://minio:9000/trusted/sptrans/live/*/*',
    '${MINIO_ACCESS_KEY}',
    '${MINIO_SECRET_KEY}',
    'Parquet');
