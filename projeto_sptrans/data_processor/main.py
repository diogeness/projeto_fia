import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

load_dotenv()

# Configurar a SparkSession para usar MinIO
def create_spark_session():
    return SparkSession.builder \
        .appName('User Raw to Trusted') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv(f"MINIO_URL", 'http://localhost:9000')) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

interval_time = int(os.getenv("INTERVAL_SECONDS_PROCESSOR", 120)) # Intervalo de tempo entre as execuções

# Path para os dados brutos do dia atual
def get_raw_day_path():
    now = datetime.now() - timedelta(hours=3)  # Ajusta o fuso horário conforme necessário
    current_year = now.year
    current_month = f"{now.month:02d}"  # Adicionar zero à esquerda
    current_day = f"{now.day:02d}"  # Adicionar zero à esquerda
    raw_base = 'sptrans/'
    return f"s3a://raw/{raw_base}year={current_year}/month={current_month}/day={current_day}/", current_year, current_month, current_day

# Função para processar os dados
def process_raw_data(spark, raw_day_path, current_year, current_month, current_day, save_path):
    # Ler os arquivos JSON no Spark (recursivamente)
    df_raw = spark.read.json(raw_day_path)

    # Verifica se há dados
    if df_raw.head(1):
        #print(df_raw.columns)
        
        # Explode e transforma os dados conforme seu código original
        df_exploded_l = df_raw.select(
            col('hr'),
            explode(col('l')).alias('l_exploded')
        )

        df_exploded_vs = df_exploded_l.select(
            col('hr'),
            col('l_exploded.c').alias('letreiro_completo'),
            col('l_exploded.cl').alias('codigo_linha'),
            col('l_exploded.sl').alias('sentido_linha'),
            col('l_exploded.lt0').alias('letreiro_destino'),
            col('l_exploded.lt1').alias('letreiro_origem'),
            col('l_exploded.qv').alias('quantidade_veiculos'),
            explode(col('l_exploded.vs')).alias('vs_exploded')
        )

        df_final = df_exploded_vs.select(
            col('hr'),
            col('letreiro_completo'),
            col('codigo_linha'),
            col('sentido_linha'),
            col('letreiro_destino'),
            col('letreiro_origem'),
            col('quantidade_veiculos'),
            col('vs_exploded.p').alias('prefixo_veiculo'),
            col('vs_exploded.a').alias('veiculo_acessivel'),
            col('vs_exploded.ta').alias('horario_informacao_utc'),
            col('vs_exploded.py').alias('lat'),
            col('vs_exploded.px').alias('long')
        )

        # Nome do arquivo Parquet baseado no caminho informado
        parquet_file_name = f"parquet_{current_year}-{current_month}-{current_day}.parquet"
        
        # Escrever o arquivo Parquet no caminho fornecido (live ou dia específico)
        df_final.write.mode('overwrite') \
            .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
            .parquet(f"{save_path}/{parquet_file_name}")
        
        print(f"DATA PROCESSOR - arquivo salvo em: {save_path}/{parquet_file_name}")
    else:
        print(f"DATA PROCESSOR - Sem arquivos json para processar em: {raw_day_path}")

# Função para encontrar o arquivo mais recente dentro do diretório do dia
def get_most_recent_file(spark, raw_day_path):
    # Listar arquivos JSON no caminho do dia atual
    df_files = spark.read.format("binaryFile").load(raw_day_path)
    
    # Filtrar para manter apenas arquivos JSON e ordenar por data de modificação
    most_recent_file = df_files.filter(df_files.path.endswith(".json")) \
                               .orderBy(col("modificationTime").desc()) \
                               .select("path").limit(1).collect()

    # Retornar o caminho do arquivo mais recente, se existir
    if most_recent_file:
        return most_recent_file[0]["path"]
    return None

if __name__ == "__main__":
    spark = create_spark_session()
    
    while True:
        try:
            start_time = time.time()

            # Obter o caminho de dados do dia atual
            raw_day_path, current_year, current_month, current_day = get_raw_day_path()

            # Processar todos os arquivos JSON do dia e salvar na pasta `trusted`
            process_raw_data(spark, raw_day_path, current_year, current_month, current_day, 
                             f"s3a://trusted/sptrans/year={current_year}/month={current_month}/day={current_day}")

            # Procurar o arquivo mais recente e salvar na pasta `live`
            most_recent_file = get_most_recent_file(spark, raw_day_path)
            print(most_recent_file)
            if most_recent_file:
                process_raw_data(spark, most_recent_file, current_year, current_month, current_day, 
                                 "s3a://trusted/sptrans/live")
            else:
                print("DATA PROCESSOR - Sem arquivos para processar.")

            # Calcular o tempo de execução
            execution_time = time.time() - start_time
            print(f"DATA PROCESSOR - Tempo Execução: {execution_time:.2f} segundos")
        except Exception as e:
            print(f"DATA PROCESSOR - Erro: {e}")

        print(f"DATA PROCESSOR - Aguarda {interval_time} segundos...")        
        time.sleep(interval_time)