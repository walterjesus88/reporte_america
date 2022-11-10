#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import time
#import pandas as pd
import dask.dataframe as dd
# Leer el archivo
print("Leyendo el archivoooo, es pesado x.x")
#df= dd.read_csv('total.csv', encoding='utf-8', engine='c', low_memory=False)
df= dd.read_csv('total.csv', low_memory=False,dtype={'User ID': 'object','Postal Code':'object','Custom Dimension 7': 'object','Buffer Ratio': 'float64',
       'Buffer Time': 'float64',
       'COMMERCIALIZATION TYPE': 'object',
       'Device Type': 'object',
       'Dimension 8': 'float64',
       'Error Description': 'object',
       'Error Metadata': 'object',
       'Happiness Score': 'float64',
       'Avg. Bitrate': 'float64',
       'Join Time': 'float64',
       'Traffic': 'float64','Domain': 'object'})
# EL df ES LA DATA COMPLETA

df['fecha filtro'] = dd.to_datetime(df['End Time']).dt.date
cols = df.columns.to_list()[:-1]


# In[ ]:


# PRIMER CONJUNTO  - 1 SOLO EXCEL
from datetime import datetime, timedelta
fecha_menor_a = input("Porfa pon aqui la fecha mayor y en formato YYYY-MM-DD: ")
delta = timedelta(days=1)
fecha_menor_a = datetime.strptime(fecha_menor_a, '%Y-%m-%d').date()

for i in range(7):
    try:
        print(i)
        print(fecha_menor_a)
        fecha_menor_a += delta
        fecha = str(fecha_menor_a)
        
        primer_filtro = df.loc[(dd.to_datetime(df['fecha filtro']) <= fecha) & (dd.to_datetime(df['fecha filtro']) >= fecha)]
        primer_filtro = primer_filtro.applymap(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)
     
        primer_filtro = primer_filtro.compute()
        primer_filtro = primer_filtro.sort_values(by=['End Time'])
        primer_filtro.to_csv(fecha+ '.csv', index=False, columns=cols)
        #primer_filtro.to_excel(fecha+ '.xlsx', index=False, columns=cols)
    except error as e: 
        print('exceso de filas excel o has ocurrido un error')
        print(e)
        #break;
