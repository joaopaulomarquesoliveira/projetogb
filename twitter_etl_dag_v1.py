import datetime
import json
import os
import shutil
import airflow
import shutil
import pandas as pd
import requests
import csv
import gcloud
from TwitterSearch import *
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from gcloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from fastavro import writer, reader, parse_schema


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
SCHEDULE = "0 0 * * *" # Isso significa que será executada todos os dias à meia noite.
TAGS_LIST = ['twitter', 'ETL']

BUCKET = 'tweet_gb' # O nome do bucket que utilizamos para armazenar os arquivos com os dados.


default_args = {
    'owner': 'Joao Paulo', 
    'start_date': YESTERDAY,
}

def upload_object_to_bucket(bucket_name: str, object_path: str, object_key=None) -> bool:
    """
    Esta função é responsável por fazer upload de um arquivo/objeto a um determinado bucket no GCS.
    Bucket_name: Nome do bucket que receberá o arquivo/objeto.
    bject_path: Caminho local para o arquivo/objeto que será enviado ao bucket.
    object_key: O caminho final do objeto dentro do bucket. Este parametro é opcional e caso não seja informado o valor do parametro object_path será assumido.
    returns: True se o upload for bem sucedido. False caso contrário.
    type: bool
    """

    file_name = (object_path if (object_key==None) else object_key)
        
    try:
        # Instanciando um novo cliente da API gcloud
        client = storage.Client('projetogb-356021')
        # Recuperando um objeto referente ao nosso Bucket
        bucket = client.get_bucket(bucket_name)
        if bucket==None:
            return False
        # Fazendo upload do objeto (arquivo) desejado
        blob = bucket.blob(file_name)
        blob.upload_from_filename(object_path)
    except Exception as e:
        print(e)
        return False
    except FileNotFoundError as e:
        print(e)
        return False
        
    return True

def extract_twitter_data(bucket_name='') -> str:
    """
    Esta função é responsável pela extração dos dados do Twitter através de uma API.

    param str bucket_name: Nome do bucket que receberá o arquivo/objeto com os dados.

    return: Retorna o object_key (caminho para o objeto AVRO dentro do bucket)
    type: str

    """   
   
    # Definindo os headers para a requisição à API
    ck='WigDvpe6ZgOZvaz1vhEAWFQmJ'
    cs='l3nrVFUgzgFmNiEumnxjLaRUpYUOXF4jH1DRy0mOSxm0P5Kso4'
    at='1546660511814672394-JrOm3BgXmQhrxgjIqPCyUiEjncbjOe'
    ats='aPQrhg9rmiq2O5omPGIFbxi1JuOLoEbJO5HjOkWFBZl7i'

    ts= TwitterSearch(
        consumer_key=ck,
        consumer_secret=cs,
        access_token=at,
        access_token_secret=ats
        )

    tso= TwitterSearchOrder()
    tso.set_keywords(['Boticario'])
    tso.set_language('pt')
    tso.set_result_type('recent')
    s=0
    Lista=[]
    
    for tweet in ts.search_tweets_iterable(tso):
        NmUser=tweet['user']['name']
        Texto=tweet['text']
        Lista.append([NmUser,Texto])
        s=s+1
        if s==50:
            break
    df = pd.DataFrame(Lista, columns = ['Name', 'Tweet'])


    # Recuperando a data e hora da execução
    date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


    path_current=os.getcwd()
    dir_local= path_current + '/twitter_data/raw'
    print(dir_local)


    # Checando a existência do diretório local para armazenar o AVRO
    try:
        if not os.path.exists(dir_local):
            os.makedirs(dir_local)
    except:
        print('não deu!')

    # Salvando o resultado da requisição em um arquivo AVRO

    # 1. Define the schema do Avro
    schema = {
        'doc': 'A weather reading.',
        'name': 'Weather',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'Name', 'type': 'string'},
            {'name': 'Tweet', 'type': 'string'},
        ],}

    parsed_schema = parse_schema(schema)

    # 2. Converta pd.DataFrame para registros - lista de dicionários
    records = df.to_dict('records')

    # 3. Write to Avro file
    file_name = path_current + f'/{date}_twitter_data.avro'
    with open(file_name, 'wb') as out:
        writer(out, parsed_schema, records)

  
    # Fazendo upload do arquivo Avro para o bucket
    object_key = f"raw/{date}_twitter_data.avro"
    #upload_object_to_bucket(bucket_name=bucket_name, object_path=file_name, object_key=object_key)


    storage_client = storage.Client('projetogb-356021')
    print(storage_client)

    bucket = storage_client.bucket('tweet_gb')
    print(bucket)
    blob = bucket.blob(object_key)

    blob.upload_from_filename(file_name)



    # Retorna o 
    full_object_key = f"{bucket_name}/{object_key}"
    return full_object_key

def load(ds, **kwargs) -> bool:
    # Recuperando o retorno da função anterior
    ti = kwargs['ti']
    avro_object_key = ti.xcom_pull(task_ids='extract_data_from_twitter_api')

    # Instanciando um novo cliente da API gcloud
    client = bigquery.Client()

    # Checa se o dataset existe. Se não existe, cria um novo dataset
    dataset_id = "twitter_Data"
    try:
        client.get_dataset(dataset_id)
        print(f"O dataset {dataset_id} já existe!")
    except NotFound:
        try:
            dataset = bigquery.Dataset(f"{client.project}.twitter_Data")
            client.create_dataset(dataset, timeout=30)
            print(f"Dataset '{dataset_id}' criado com sucesso!")
        except Exception as e:
            print(e)
            return False




    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO)
    # Definindo nova tabela
    table_id = f"{client.project}.{dataset_id}.tweet"
    uri = f'gs://{avro_object_key}'

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))



    
    """
    try:
    
        # Definindo nova tabela
        table_id = f"{client.project}.{dataset_id}.tweet"

        destination_table = client.get_table(table_id)
    except Exception as e:
            print(e)
            return False
    try:

        # Definindo a configuração do Job
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO)

        # Definimos a URI do nosso objeto .csv transformado dentro do bucket
        uri = f"gs://tweet_gb"

        # Iniciamos o job que vai carregar os dados para dentro da nossa tabela no BigQuery
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result()

        
        # Capturando o número de registros na tabela depois de realizar o load
        query_count = f"SELECT COUNT(*) FROM {table_id}"
        query_count_job = client.query(query_count)
        end_count = 0
        for row in query_count_job:
            end_count=end_count+row[0]   

        print(f"{end_count-start_count} novos registros em {table_id}!")
        

    except:
        None
        return False
    """
    
    return True

def load_base2017():

    # Instanciando um novo cliente da API gcloud
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ID_MARCA", "INTEGER"), 
            bigquery.SchemaField("MARCA", "STRING"), 
            bigquery.SchemaField("ID_LINHA", "INTEGER"), 
            bigquery.SchemaField("LINHA", "STRING"),  
            bigquery.SchemaField("DATA_VENDA", "STRING"), 
            bigquery.SchemaField("QTD_VENDA", "INTEGER"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )


    # Definindo nova tabela
    table_id = f"{client.project}.bases_gb.base2017_v1"
    uri = f'gs://projeto_gb/Base 2017.csv'

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


with airflow.DAG('twitter_etl_dag_v1', schedule_interval=SCHEDULE, tags=TAGS_LIST, default_args=default_args, catchup=False) as dag:

    inicio = DummyOperator(
        task_id='inicio'
    )

    extraction = PythonOperator(
        task_id = 'extract_data_from_twitter_api',
        python_callable = extract_twitter_data,
        op_kwargs={"bucket_name":BUCKET}
    )
    
    load_data_into_bq = PythonOperator(
        task_id = 'load_data_into_bigquery',
        python_callable = load,
    )


    load_base2017_into_bq = PythonOperator(
        task_id = 'load_base2017_into_bq',
        python_callable = load_base2017,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule='none_failed'
    )

    # Dependencia entre as tarefas
    inicio >> extraction >> [load_data_into_bq, load_base2017_into_bq] >> fim