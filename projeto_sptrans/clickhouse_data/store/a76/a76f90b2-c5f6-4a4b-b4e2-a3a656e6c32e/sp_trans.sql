ATTACH VIEW _ UUID 'f5cfa76f-36ab-4ff7-b778-17b89007a2fe'
(
    `hr` Nullable(String),
    `letreiro_completo` Nullable(String),
    `codigo_linha` Nullable(Int64),
    `sentido_linha` Nullable(Int64),
    `letreiro_destino` Nullable(String),
    `letreiro_origem` Nullable(String),
    `quantidade_veiculos` Nullable(Int64),
    `prefixo_veiculo` Nullable(Int64),
    `veiculo_acessivel` Nullable(Bool),
    `horario_informacao_utc` Nullable(String),
    `lat` Nullable(Float64),
    `long` Nullable(Float64)
)
AS SELECT *
FROM s3('http://minio:9000/trusted/sptrans/*/*/*/*/*', 'uJ2LAihTN0YWgRmh5Wdd', '5XlZJa03fIrs1hkaB5v7QZEEPqAzFSpchMVSyobD', 'Parquet')
