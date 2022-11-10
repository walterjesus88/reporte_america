import pandas as pd
import datetime 
# Leer el archivo
print("Leyendo el archivoooo, es pesado x.x")
df= pd.read_csv('2022-11-03.csv', encoding='utf-8', engine='c', low_memory=False)

# EL df ES LA DATA COMPLETA
#df['fecha filtro'] = pd.to_datetime(df['End Time']).dt.date
df['fecha filtro'] = pd.to_datetime(df['End Time'], format='%Y%m%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')
cols = df.columns.to_list()[:-1]
print(df)

# PRIMER CONJUNTO  - 1 SOLO EXCEL
print("Listo Gabicita empecemos el primer filtro")
fecha_menor_a = input("Porfa pon aqui la fecha mayor y en formato YYYY-MM-DD: hh:mm:ss")
print("Gracias preciosa :)")

fecha_mayor_a = input("Porfa pon aqui la fecha menor y en formato YYYY-MM-DD: hh:mm:ss")

# Slicing el tramo que nos interesa
print("Uhh ya esta filtrandoo y convertiendoooo... ")
primer_filtro = df.loc[(pd.to_datetime(df['fecha filtro']) <= fecha_menor_a) & (pd.to_datetime(df['fecha filtro']) >= fecha_mayor_a)]

primer_filtro = primer_filtro.applymap(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)

primer_filtro.to_csv('primer_filtro.csv', index=False, columns=cols)
print('Listo! primer archivo realizado')
4
# SEGUNDO CONJUNTO  - 1 SOLO EXCEL
print("Para el segundo filtro")
fecha_menor_a_2 = input("Porfa pon aqui la fecha mayor y en formato YYYY-MM-DD: ")
fecha_mayor_a_2 = input("Porfa pon aqui la fecha menor y en formato YYYY-MM-DD: ")

print("Convertiendooo el segundo filtroooo...")
segundo_filtro = df.loc[(pd.to_datetime(df['fecha filtro']) <= fecha_menor_a_2) & (pd.to_datetime(df['fecha filtro']) >= fecha_mayor_a_2)]

segundo_filtro = segundo_filtro.applymap(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)

segundo_filtro.to_csv('segundo_filtro.csv', index=False, columns=cols)
print('Acabamos! segundo archivo realizado')
print('Listo que tengas un buen día :)')
