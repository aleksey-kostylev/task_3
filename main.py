# import jellyfish as jf
# import numpy as np

import mysql.connector
import pandas as pd
import random
from sqlalchemy import create_engine
from thefuzz import fuzz
import time
from tqdm import tqdm

from config import USER, HOST, PASS, DB_NAME
from eidos import NameProcessing

connection = mysql.connector.connect(host=HOST, user=USER, passwd=PASS, db=DB_NAME)
outlets = pd.read_sql_query("SELECT * FROM `outlets`", connection)
outlets_clean = pd.read_sql_query("SELECT * FROM `outlets_clean`", connection)
connection.close()

outlets = outlets.set_index('id')
new_df = outlets
new_df['cluster_id'] = 0

# Создают случайный код в формате XXXX-XXXX-XXXX-XXXX
def cluster_id():
    id = ''

    for x in range(16):
        id = id + random.choice(list('1234567890abcdefghigklmnopqrstuvyxwzABCDEFGHIGKLMNOPQRSTUVYXWZ'))
    
        if len(id) == 4:
            id += '-'
    
        if len(id) == 9:
            id += '-'
        
        if len(id) == 14:
            id += '-'

    return id

# Функция для обработки DataFrame объекта
def DataProcessing(data, counter=1, read_from='Торг_точка_грязная', write_to='cluster_id', method=fuzz.ratio): 
    counter_f = counter
    for i in tqdm(data[read_from]):
        s = NameProcessing(i)
        k = s.cleaned_string
        if data[write_to].loc[counter_f] == 0:
            code = cluster_id()
            for j in range(len(data[read_from])):
                n = NameProcessing(data[read_from][j+counter])
                m = n.cleaned_string
                if method(k, m) == 100:
                    data[write_to].loc[j+counter] = code
            counter_f += 1
        else:
            counter_f += 1

# Функция для обработки DataFrame объекта
def SubclusterProcessing(data, counter=1, read_from='Торг_точка_грязная', write_to='subcluster_id', method=fuzz.WRatio):
    counter_f = counter
    for i in data[read_from]:
        s1 = data[read_from][counter_f]
        if data[write_to].loc[counter_f] == 0:
            code = cluster_id()
            for j in range(len(data[read_from])):
                s2 = data[read_from][j+counter]
                if method(s1, s2) > 95:
                    new_df[write_to].loc[data['id'].loc[j+counter]] = code
            counter_f += 1
        else:
            counter_f += 1

DataProcessing(new_df, counter=1, read_from='Торг_точка_грязная', write_to='cluster_id', method=fuzz.ratio)

new_df['subcluster_id'] = 0

main_cluster_list = []
for c in new_df['cluster_id']:
    main_cluster_list.append(c)

main_cluster_list = list(set(main_cluster_list))
print(f'main_cluster_list_len: {len(main_cluster_list)}')

for cluster in tqdm(main_cluster_list):
    df = new_df[new_df['cluster_id']==cluster].reset_index()
    SubclusterProcessing(df, counter=0, read_from='Торг_точка_грязная', write_to='subcluster_id', method=fuzz.WRatio)
    #print(f'Cluster {cluster} done!')
    time.sleep(0.1)

sub_cluster_list = []
for sc in new_df['subcluster_id']:
    sub_cluster_list.append(sc)

sub_cluster_list = list(set(sub_cluster_list))
print(f'sub_cluster_list_len: {len(sub_cluster_list)}')

engine = create_engine(f"mysql+mysqldb://{USER}:{PASS}@{HOST}/{DB_NAME}")
new_df.to_sql('new_df', con = engine)

new_df.to_csv('new_df.csv', sep=';')