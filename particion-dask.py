import pandas as pd
import dask.dataframe as dd
# Leer el archivo
print("Leyendo el archivoooo, es pesado x.x")
#df= dd.read_csv('total.csv', encoding='utf-8', engine='c', low_memory=False)
df= dd.read_csv('total.csv', low_memory=False,dtype={'User ID': 'object','Postal Code':'object','Custom Dimension 7':'object','Error Description':'object','Error Metadata':'object','Domain':'object','COMMERCIALIZATION TYPE': 'object','Device Type': 'object','Dimension 8': 'float64','Buffer Ratio': 'float64','Buffer Time': 'float64','Happiness Score': 'float64','Avg. Bitrate': 'float64',
       'Join Time': 'float64','Traffic': 'float64','Dimension 10':'object'})
print(df)
# EL df ES LA DATA COMPLETA
df['fecha filtro'] = dd.to_datetime(df['End Time']).dt.date
cols = df.columns.to_list()[:-1]

# PRIMER CONJUNTO  - 1 SOLO EXCEL
print("Listo empecemos el primer filtro")
fecha_menor_a = input("Porfa pon aqui la fecha mayor y en formato YYYY-MM-DD: ")
print("Gracias :)")

fecha_mayor_a = input("Porfa pon aqui la fecha menor y en formato YYYY-MM-DD: ")

# Slicing el tramo que nos interesa
print("Uhh ya esta filtrandoo y convertiendoooo... ")
primer_filtro = df.loc[(dd.to_datetime(df['fecha filtro']) <= fecha_menor_a) & (dd.to_datetime(df['fecha filtro']) >= fecha_mayor_a)]

primer_filtro = primer_filtro.applymap(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)
print(primer_filtro)
primer_filtro=primer_filtro.compute()
primer_filtro=primer_filtro.sort_values(by='End Time')
primer_filtro.to_csv(fecha_mayor_a+'.csv', index=False, columns=cols)
print('Listo! primer archivo realizado')

### SEGUNDO CONJUNTO  - 1 SOLO EXCEL
##print("Para el segundo filtro")
##fecha_menor_a_2 = input("Porfa pon aqui la fecha mayor y en formato YYYY-MM-DD: ")
##fecha_mayor_a_2 = input("Porfa pon aqui la fecha menor y en formato YYYY-MM-DD: ")
##
##print("Convertiendooo el segundo filtroooo...")
##segundo_filtro = df.loc[(dd.to_datetime(df['fecha filtro']) <= fecha_menor_a_2) & (dd.to_datetime(df['fecha filtro']) >= fecha_mayor_a_2)]
##segundo_filtro = segundo_filtro.applymap(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)
###segundo_filtro.to_csv('segundo_filtro.csv', index=False, columns=cols)
##segundo_filtro=segundo_filtro.compute()
##segundo_filtro=segundo_filtro.sort_values(by='End Time')
##segundo_filtro.to_csv(fecha_mayor_a_2+'.csv', index=False, columns=cols)
##print('Acabamos! segundo archivo realizado')
##print('Listo que tengas un buen día :)')
