from pyspark.sql import SparkSession

# Crear sesión de Spark
#spark = SparkSession.builder.appName("PySpark to SQL Server").getOrCreate()

#config("spark.ui.port", "4041")

spark = SparkSession.builder.appName("appuno").config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()        
        #.config("spark.master", "local[4]") \
        #.config("spark.shuffle.sql.partitions",2) \

# Crear DataFrame de ejemplo
#df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "value"])

#df = spark.read.csv('DISPOSITIVOS_2209.txt',sep="|", header=True, inferSchema=True)
#spark.sparkContext.addPyFile("C:\Users\wrivera\Downloads\sqljdbc_12.2\enu\mssql-jdbc-12.2.0.jre8.jar")
#print(df)

#Configurar opciones para la conexión JDBC
jdbcHostname = "200.121.226.150"
jdbcDatabase = "BACKUP_PRUEBAS_WR"
jdbcPort = 1433
jdbcUsername = "Walter"
jdbcPassword = "Walter901!.!"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

# # Definir tabla de destino
#tableName = "Dispositivos"

# # Escribir datos en la tabla de destino
# df.write \
#     .format("jdbc") \
#     .option("url", jdbcUrl) \
#     .option("dbtable", tableName) \
#     .option("user", jdbcUsername) \
#     .option("password", jdbcPassword) \
#     .mode("append") \
#     .save()


import pyodbc

# server = '200.121.226.150'
# database = 'BACKUP_PRUEBAS_WR'
# username = 'Walter'
# password = 'Walter901!.!'
# driver = '{SQL Server Native Client 11.0}'
# connection_string = f"""DRIVER={driver};SERVER={server};
#                       DATABASE={database};UID={username};
#                       PWD={password}"""

# cnxn = pyodbc.connect(connection_string)

table_name = 'Dispositivos'
# df = spark.read \
#      .format("jdbc") \
#      .option("url", jdbcUrl) \
#      .option("dbtable", table_name) \
#      .option("user", jdbcUsername) \
#      .option("password", jdbcPassword) \
#      .load()


print('vfdgd')

df = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://200.121.226.150:1433;databaseName={jdbcDatabase};") \
    .option("dbtable", table_name) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()


# print(df)


# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("Read from SQL Server") \
#     .config("spark.driver.extraClassPath", "C:\\Users\\wrivera\\Downloads\\sqljdbc_12.2\\enu\\mssql-jdbc-12.2.0.jre8.jar") \
#     .getOrCreate()

# url = "jdbc:sqlserver://200.121.226.150:1433;databaseName=BACKUP_PRUEBAS_WR"
# table_name = "Dispositivos"
# properties = {
#     "user": "Walter",
#     "password": "Walter901!.!",
#     "driver": 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
# }

# df = spark.read.jdbc(url=jdbcUrl, table=table_name, properties=properties)
# df.show()
