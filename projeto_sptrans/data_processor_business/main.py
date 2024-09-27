import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, udf
from pyspark.sql.types import DoubleType
from pyspark.sql import Window
from pyspark.sql.functions import lag
import math
import pyspark.sql.functions as F

load_dotenv()

# Configurar a SparkSession para usar MinIO
def create_spark_session():
    return SparkSession.builder \
        .appName('User Trusted to Business') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_URL", 'http://localhost:9000')) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.instances", "4") \
        .getOrCreate()

# Função Haversine
def haversine(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    R = 6371  # Raio da Terra em km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2

    # Garantir que 'a' esteja no intervalo de [0, 1]
    a = min(1, max(0, a))
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# Registrando UDF Haversine
haversine_udf = udf(haversine, DoubleType())

# Função principal de processamento
def process_data():
    spark = create_spark_session()
    interval_time = int(os.getenv("INTERVAL_SECONDS_PROCESSOR", 120))

    while True:
        try:
            start_time = time.time()
            
            # Obtendo a data formatada
            now = datetime.now() - timedelta(hours=3)
            formatted_date = now.strftime('%Y-%m-%d')

            # Caminho para leitura do parquet
            path = f"s3a://trusted/sptrans/live/parquet_{formatted_date}.parquet"

            # Leitura dos dados
            trusted_olho_vivo = spark.read.parquet(path)

            # Leitura de dados GTFS
            gtfs_stops_df = spark.read.csv("s3a://trusted/gtfs/stops.txt", header=True, inferSchema=True)
            gtfs_routes_df = spark.read.csv("s3a://trusted/gtfs/routes.txt", header=True, inferSchema=True)
            gtfs_trips_df = spark.read.csv("s3a://trusted/gtfs/trips.txt", header=True, inferSchema=True)
            gtfs_stop_times_df = spark.read.csv("s3a://trusted/gtfs/stop_times.txt", header=True, inferSchema=True)

            # Usando Broadcast para otimizar a operação
            gtfs_stops_df = broadcast(gtfs_stops_df)

            # Cruzamento de dados
            cruzamento_rotas = trusted_olho_vivo.join(gtfs_trips_df, trusted_olho_vivo.letreiro_completo == gtfs_trips_df.route_id)

            # Encontrar a parada mais próxima com base nas coordenadas
            onibus_paradas_df = trusted_olho_vivo.crossJoin(gtfs_stops_df) \
                .withColumn("distancia", haversine_udf(col("lat"), col("long"), col("stop_lat"), col("stop_lon")))

            # Cruzamento de dados com trips e rotas
            gtfs_stop_times_stops = gtfs_stop_times_df.join(gtfs_stops_df, on="stop_id")
            gtfs_stop_times_trips = gtfs_stop_times_stops.join(gtfs_trips_df, on="trip_id")
            gtfs_stop_times_trips_route = gtfs_stop_times_trips.join(gtfs_routes_df, on="route_id")
            gtfs_stop_times_trips_route_api = gtfs_stop_times_trips_route.join(
                trusted_olho_vivo,
                gtfs_stop_times_trips_route.route_id == trusted_olho_vivo.letreiro_completo,
                "inner"
            )

            # Janela para pegar a próxima parada
            windowSpec = Window.partitionBy("trip_id").orderBy("stop_sequence")

            # Calculando a distância entre paradas consecutivas
            df_distancia = gtfs_stop_times_trips_route.withColumn("prox_lat", lag("stop_lat", -1).over(windowSpec)) \
                         .withColumn("prox_lon", lag("stop_lon", -1).over(windowSpec)) \
                         .withColumn("distancia_km", haversine_udf(col("stop_lat"), col("stop_lon"), col("prox_lat"), col("prox_lon")))

            # Definindo a janela para calcular a última sequência
            window_spec = Window.partitionBy("route_id")

            # Adicionando a última sequência de paradas ao DataFrame
            paradas_com_ultima_sequencia = gtfs_stop_times_trips_route_api.withColumn(
                "ultima_sequencia",
                F.max("stop_sequence").over(window_spec)
            )

            # Adicionando a distância das paradas até a posição atual
            paradas_com_distancia = paradas_com_ultima_sequencia.withColumn(
                "distancia_atual",
                haversine_udf(F.col("lat"), F.col("long"), F.col("stop_lat"), F.col("stop_lon"))
            )

            # Filtrando as paradas até a última parada
            paradas_faltantes = paradas_com_distancia.filter(
                (F.col("stop_sequence") > 0) & 
                (F.col("stop_sequence") <= F.col("ultima_sequencia"))
            )

            # Contando as paradas faltantes por route_id, sentido_linha e prefixo_veiculo
            total_paradas_faltantes_por_route = paradas_faltantes.groupBy("route_id", "sentido_linha", "prefixo_veiculo").agg(
                F.count("*").alias("total_paradas_faltantes")
            )

            # Escrevendo os resultados
            cruzamento_rotas.write.parquet('s3a://business/cruzamento_rotas/', mode='overwrite')

            gtfs_stop_times_trips_route_api.write.parquet('s3a://business/gtfs_stop_times_trips_route_api/', mode='overwrite')

            df_distancia.write.parquet('s3a://business/df_distancia/', mode='overwrite')

            paradas_com_ultima_sequencia.write.parquet('s3a://business/paradas_com_ultima_sequencia/', mode='overwrite')

            trusted_olho_vivo.write.parquet('s3a://business/trusted_olho_vivo/', mode='overwrite')

            total_paradas_faltantes_por_route.write.parquet('s3a://business/total_paradas_faltantes_por_route/', mode='overwrite')

            # Calcular o tempo de execução
            execution_time = time.time() - start_time
            print(f"DATA PROCESSOR BUSINESS - Tempo Execução total: {execution_time:.2f} segundos")

        except Exception as e:
            print(f"DATA PROCESSOR BUSINESS - Erro: {e}")

        print(f"DATA PROCESSOR BUSINESS - Aguardando {interval_time} segundos...")
        time.sleep(interval_time)

if __name__ == "__main__":
    process_data()
