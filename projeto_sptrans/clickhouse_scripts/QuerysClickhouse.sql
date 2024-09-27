create database sptrans;

CREATE VIEW cruzamento_rotas AS (
SELECT
   *
FROM s3(
    'http://minio:9000/business/cruzamento_rotas/*',
    'datalake',
    'datalake',
    'Parquet'
));



CREATE VIEW distancia AS (
SELECT
   *
FROM s3(
    'http://minio:9000/business/df_distancia/*',
    'datalake',
    'datalake',
    'Parquet'
))

CREATE VIEW shapes AS (
SELECT
   *
FROM s3(
    'http://minio:9000/business/df_shape/*',
    'datalake',
    'datalake',
    'Parquet'
))



CREATE VIEW gtfs_stop_times_trips_route AS
SELECT
   *
FROM s3(
    'http://minio:9000/business/gtfs_stop_times_trips_route_api/*',
    'datalake',
    'datalake',
    'Parquet'
)


CREATE VIEW trusted_olho_vivo AS
SELECT
   *
FROM s3(
    'http://minio:9000/business/trusted_olho_vivo/*',
    'datalake',
    'datalake',
    'Parquet'
)


CREATE VIEW total_paradas_faltantes_por_route AS
SELECT
   *
FROM s3(
    'http://minio:9000/business/total_paradas_faltantes_por_route/*',
    'datalake',
    'datalake',
    'Parquet'
)


