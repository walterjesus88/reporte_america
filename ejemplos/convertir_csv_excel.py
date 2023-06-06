#from pyspark.sql import SparkSession
#from pyspark.conf import SparkConf

#conf = SparkConf().setAppName("Mi aplicación").set("spark.ui.port", "4041")
#spark = SparkSession.builder.config(conf=conf).getOrCreate()
#spark = SparkSession.builder.appName("PySpark to SQL Server").config("spark.ui.port", "4041").getOrCreate()



#spark = SparkSession.builder.appName("Mi aplicación").getOrCreate()
#spark = SparkSession.builder.appName("Mi app").config("spark.jars.packages", "com.crealytics:spark-excel_2.12-3.3.1_0.18.7").getOrCreate()

# Cargar el archivo CSV en un DataFrame de PySpark
#df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("2023-04-14.csv")
#df = spark.read.csv('subida.csv',sep="|", header=True, inferSchema=True)
#spark.sparkContext.addPyFile("C:\Users\wrivera\Downloads\sqljdbc_12.2\enu\mssql-jdbc-12.2.0.jre8.jar")
#print('dasdasfklnasflnk')
#print(df)


from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
#df = spark.createDataFrame([(1,'a'),(2,'b'),(3,'c')], ['id', 'letter'])
#df.show()


df = spark.read.csv("2023-04-14.csv", header=True, inferSchema=True)

df.show()

print(spark)

spark.stop()
#df.to_excel("output.xlsx")  
#df=df.toPandas()
#df.to_csv('ssss.csv')

#Escribir el DataFrame en un archivo Excel
##df.write.format("com.crealytics.spark.excel") \
##    .option("header", "true") \
##    .option("dataAddress", "'Sheet1'!A1") \
##    .mode("overwrite") \
##    .save("archivo.xlsx")



