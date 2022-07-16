import requests
import csv
import pandas as pd
import json
from TwitterSearch import *


ck='WigDvpe6ZgOZvaz1vhEAWFQmJ'
cs='l3nrVFUgzgFmNiEumnxjLaRUpYUOXF4jH1DRy0mOSxm0P5Kso4'
at='1546660511814672394-JrOm3BgXmQhrxgjIqPCyUiEjncbjOe'
ats='aPQrhg9rmiq2O5omPGIFbxi1JuOLoEbJO5HjOkWFBZl7i'


def ingestao(df):
        #Abertura da conexão com o DB.
        con=mysql.connector.connect(host='localhost',user='root',password='341341Jp@', database='projetogb')
        cursor = con.cursor()
        
        #Criação do database, caso não estejá criado.
        cursor.execute('''
        CREATE DATABASE IF NOT EXISTS projetogb
               ''')
        
        #Declaração do schema da tabela.
        text='USE projetogb;\
        CREATE TABLE if not exists tweet( \
        NOME      varchar(50),\
        TEXTO     varchar(200))'
        
        cursor.execute(text)

        
        
        cursor.close()
        con=mysql.connector.connect(host='localhost',user='root',password='341341Jp@', database='projetogb')
        cursor = con.cursor()
        
        
        #Alimentação da tabela com o metodo insert.
        for row in df.itertuples():
            k='INSERT INTO tweet (NOME, TEXTO) VALUES (%s,%s)'
            val = (row.Name, row.Tweet)
            cursor.execute(k, val)
        
        con.commit()




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
#df.to_csv('/home/joao/Área de Trabalho/projetogb/tweet1.csv', index=None, encoding= 'utf-8')
ingestao(df)
