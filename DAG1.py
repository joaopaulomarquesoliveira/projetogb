"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
import requests
import csv
import os
import pandas as pd
import json
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from datetime import datetime, timedelta
from TwitterSearch import *

ck='WigDvpe6ZgOZvaz1vhEAWFQmJ'
cs='l3nrVFUgzgFmNiEumnxjLaRUpYUOXF4jH1DRy0mOSxm0P5Kso4'
at='1546660511814672394-JrOm3BgXmQhrxgjIqPCyUiEjncbjOe'
ats='aPQrhg9rmiq2O5omPGIFbxi1JuOLoEbJO5HjOkWFBZl7i'

def funcao():
    try:
    
        ts= TwitterSearch(
            consumer_key=ck,
            consumer_secret=cs,
            access_token=at,
            access_token_secret=ats
            )
        
        tso= TwitterSearchOrder()
        tso.set_keywords(['Boticario', 'maquiagem'])
        tso.set_language('pt')
        tso.set_result_type('recent')
        s=0
        Lista=[]
    
        
        for tweet in ts.search_tweets_iterable(tso):
            NmUser=(json.loads((json.dumps(tweet))))['user']['name']
            Texto=(json.loads((json.dumps(tweet))))['text']
            Lista.append([NmUser,Texto])
            s=s+1
            if s==10:
                break
        df = pd.DataFrame(Lista, columns = ['Name', 'Tweet'])
        os.chdir(f'/home/joaogcp12/')
        df.to_csv('teste.csv')

            
    except:
        None
        




    
with DAG('teste', start_date = datetime(2022, 7, 12),
schedule_interval = '00 18 * * *', catchup = False) as dag:

    funcao = PythonOperator(
        task_id = 'funcao',
        python_callable = funcao
        )

    