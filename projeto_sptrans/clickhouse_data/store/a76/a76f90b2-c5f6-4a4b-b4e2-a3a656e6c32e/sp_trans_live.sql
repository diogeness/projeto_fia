ATTACH VIEW _ UUID 'd6dc328e-a3ee-48bb-ab0e-472357af9f88'
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
    `long` Nullable(Float64),
    `qtd` UInt8
)
AS SELECT
    *,
    1 AS qtd
FROM s3('http://minio:9000/trusted/sptrans/live/*/*', 'uJ2LAihTN0YWgRmh5Wdd', '5XlZJa03fIrs1hkaB5v7QZEEPqAzFSpchMVSyobD', 'Parquet')
