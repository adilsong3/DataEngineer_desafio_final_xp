from operator import index
from sched import scheduler
from urllib import response
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests
import json
import boto3
import pymongo
from sqlalchemy import create_engine
from airflow.models import Variable

# Pegando as senhas cadastradas no airflow já criptografado
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
mongo_password_igti = Variable.get("mongo_password_igti")

# criando client para conectar a aws
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# definir default_args
default_args = {
    "owner" : "Adilson Gustavo",
    "depends_on_past" : False,
    "start_date" : datetime(2022, 7, 29)
}

# Criando a dag
@dag(default_args=default_args, schedule_interval="@once", catchup=False, description="Desafio Final IGTI EDD", tags=["IGTI","EDD","mongodb","python","airflow"])
def igti_desafio_final_edd():

    """
    Flow para oberter dados do IBGE via api e de uma base MongoDB.
    Depositar no datalake s3 e no DW PostgreSQL local
    """

    @task
    def extract_mongo():
        data_path = "/tmp/pnadc20203.csv"
        client = pymongo.MongoClient(f"mongodb+srv://estudante_igti:{mongo_password_igti}@unicluster.ixhvw.mongodb.net/ibge?retryWrite=true&w=majority")
        db = client.ibge
        pnad_collec = db.pnadc20203
        df = pd.DataFrame(list(pnad_collec.find()))
        df.to_csv(data_path, index=False, encoding="utf-8", sep=";")
        return data_path

    @task
    def data_check(file_name):
        df = pd.read_csv(file_name, sep=";")
        print(df)

    @task
    def extract_api():
        data_path = "/tmp/dimensao_mesorregioes_mg.csv"
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/MG/mesorregioes"
        response = requests.get(url)
        response_json = json.loads(response.text)
        df = pd.DataFrame(response_json)[["id", "nome"]]
        df.to_csv(data_path, index=False, encoding="utf-8", sep=";")
        return data_path

    @task
    def upload_to_s3(file_name):
        print(f"Got filename: {file_name}")
        print(f"Got object_name: {file_name[5:]}")
        s3_client.upload_file(file_name, "datalake-adilson-desafio-igti-edd", f"raw-data/desafio_final_edd/{file_name[5:]}")

    @task
    def write_to_postgres(csv_file_path):
        engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
        df = pd.read_csv(csv_file_path, sep=";")
        if csv_file_path == "/tmp/pnadc2023.csv":
            df = df.loc[(df.idade >= 20) & (df.idade <= 40) & (df.sexo == "Mulher")]
        df.to_sql(csv_file_path[5:-4], engine, if_exists="replace", index=False, method="multi", chunksize=1000)

    
    # Orquestração no airflow
    mongo = extract_mongo()
    api = extract_api()

    checagem_mongo = data_check(mongo)

    upmongo = upload_to_s3(mongo)
    upapi = upload_to_s3(api)

    wrmongo = write_to_postgres(mongo)
    wrapi = write_to_postgres(api)

    checagem_mongo >> [upmongo , wrmongo]

execucao = igti_desafio_final_edd()