from pyspark.sql import SparkSession

# create a SparkSession
spark = SparkSession.builder.appName('CSV to Excel').getOrCreate()

# read the CSV file into a Spark DataFrame
df = spark.read.csv('2023-04-12.csv', header=True, inferSchema=True)

# write the Spark DataFrame to an Excel file
df.write.format('com.crealytics.spark.excel') \
  .option('header', 'true') \
  .option('dataAddress', '\'Sheet1\'!A1') \
  .mode('overwrite') \
  .save('output_file.xlsx')
