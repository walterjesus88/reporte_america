#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import time
import pandas as pd
import dask.dataframe as dd
# Leer el archivo
print("Leyendo el archivoooo, es pesado x.x")

df= pd.read_csv('2023-01-10.csv',encoding='utf-8', engine='c', low_memory=False)

##        low_memory=False,dtype={'User ID': 'object','Postal Code':'object','Custom Dimension 7': 'object','Buffer Ratio': 'float64',
##       'Buffer Time': 'float64',
##       'COMMERCIALIZATION TYPE': 'object',
##       'Device Type': 'object',
##       'Dimension 8': 'float64',
##       'Error Description': 'object',
##       'Error Metadata': 'object',
##       'Happiness Score': 'float64',
##       'Avg. Bitrate': 'float64',
##       'Join Time': 'float64',
##       'Traffic': 'float64','Domain': 'object'})
# EL df ES LA DATA COMPLETA

df.to_excel('filtro.xlsx', index=False)
