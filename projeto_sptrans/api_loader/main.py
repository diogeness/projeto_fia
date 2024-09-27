import requests
from minio import Minio
import os
from dotenv import load_dotenv
import json
import io
import pytz
from datetime import datetime
import time

load_dotenv()

sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

# Conexao com MinIO
minio_client = Minio(
    os.getenv("MINIO_HOSTNAME", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

interval_time = int(os.getenv("INTERVAL_SECONDS", 120))

def save_raw_data(bucket_name, position):
    """
    Save raw data to a specified bucket in MinIO.
    This function takes a bucket name and a position dictionary, converts the 
    position to a JSON string, and uploads it to the specified bucket in MinIO 
    with a timestamped filename.
    Args:
        bucket_name (str): The name of the bucket where the data will be saved.
        position (dict): The position data to be saved, which will be converted 
                         to a JSON string.
    Raises:
        Exception: If there is an error during the upload process.
    """
    
    json_position = json.dumps(position)
    json_bytes = io.BytesIO(json_position.encode("utf-8"))
    len_json = len(json_position)

    dir_path = datetime.now(sao_paulo_tz).strftime("sptrans/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")
    filename = datetime.now(sao_paulo_tz).isoformat(timespec='milliseconds')
    object_name = dir_path + "position_" + filename + ".json"

    minio_client.put_object(bucket_name, object_name, json_bytes, len_json)
    print(f"API LOADER - arquivo salvo em: {bucket_name}/{object_name}")

class RequestAPI:
    def __init__(self):
        self.url = os.getenv("API_URL")
        self.token = os.getenv("API_TOKEN")
        self.session = requests.Session()

    def authentication(self):
        auth_url = f"{self.url}/Login/Autenticar?token={self.token}"
        response = self.session.post(auth_url)
        print(f"API LOADER - Retorno da API: {response.text}")

    def position(self):
        position_url = f"{self.url}/Posicao"
        response = self.session.get(position_url)
        return response.json()

if __name__ == "__main__":
    while True:
        start_time = time.time() 
        try:
            api = RequestAPI()
            api.authentication()
            position_data = api.position()
            print(f"API LOADER - Tempo de resposta da API: {time.time() - start_time:.2f} segundos")
            save_raw_data("raw", position_data)
        except Exception as e:
            print(f"API LOADER - Erro: {e}")

        execution_time = time.time() - start_time
        print(f"API LOADER - Tempo Execução: {execution_time:.2f} segundos")    
        print(f"API LOADER - Aguarda {interval_time} segundos...")    
        time.sleep(interval_time)