import pandas as pd
import json
import csv
import mysql.connector
from pathlib import Path
import os
import re


txt=os.listdir('/home/joao/Área de Trabalho/projetogb') #Lista os arquivos do diretório referido


#Laço para reconhecer quais arquivos são .xlsx e proceguir com a ingestão.
for arq in txt:
    if re.findall(".xlsx", arq):
        print(arq)
        
        file_name = (Path(arq).stem).replace(' ','') #Manobra para colher o nome do arquivo.
        
        
        #Criação do datframe do arquivo .xlsx.
        data=pd.read_excel('/home/joao/Área de Trabalho/projetogb/'+arq)
        df=pd.DataFrame(data)
        
        
        #Abertura da conexão com o DB.
        con=mysql.connector.connect(host='localhost',user='root',password='341341Jp@', database='projetogb')
        cursor = con.cursor()
        
        #Criação do database, caso não estejá criado.
        cursor.execute('''
        CREATE DATABASE IF NOT EXISTS projetogb
               ''')
        
        #Declaração do schema da tabela.
        text='USE projetogb;\
        CREATE TABLE if not exists {}( \
        ID_MARCA      int,\
        MARCA         varchar(50),\
        ID_LINHA      int,\
        LINHA         varchar(50),\
        DATA_VENDA    datetime,\
        QTD_VENDA     int)'.format(file_name)
        
        cursor.execute(text)
        
        
        cursor.close()
        con=mysql.connector.connect(host='localhost',user='root',password='341341Jp@', database='projetogb')
        cursor = con.cursor()
        
        
        #Alimentação da tabela com o metodo insert.
        for row in df.itertuples():
            k='INSERT INTO {} (ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA ) VALUES (%s,%s,%s,%s,%s,%s)'.format(file_name)
            val = (row.ID_MARCA, row.MARCA, row.ID_LINHA,row.LINHA, row.DATA_VENDA, row.QTD_VENDA)
            cursor.execute(k, val)
        
        con.commit()
