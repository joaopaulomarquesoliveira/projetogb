import requests
import csv
import pandas as pd
import json
from TwitterSearch import *
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
tso.set_keywords(['Boticario', 'maquiagem'])
tso.set_language('pt')
tso.set_result_type('recent')
s=0
Lista=[]
    
for tweet in ts.search_tweets_iterable(tso):
    NmUser=str(json.load((json.dumps(tweet)))['user']['name'])
    Texto=str(json.load((json.dumps(tweet)))['text']).rstrip('\n')
    Lista.append([NmUser,Texto])
    s=s+1
    if s==2:
        break
df = pd.DataFrame(Lista, columns = ['Name', 'Tweet'])
df.to_csv(r'C:\Users\jpmarques\Desktop\tweet1.csv', index=None, encoding= 'utf-8')
