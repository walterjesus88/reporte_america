

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

# Print the tables in the catalog
print(my_spark.catalog.listTables())

df = my_spark.read.csv("subida2.csv", header=True, inferSchema=True)

df.show()

import numpy as np
import pandas as pd

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
#spark_temp = my_spark.createDataFrame(pd_temp)

### Examine the tables in the catalog
##print(my_spark.catalog.listTables())
##
### Add spark_temp to the catalog
##spark_temp.createOrReplaceTempView('temp')
##
### Examine the tables in the catalog again
##print(my_spark.catalog.listTables())


from pyspark.sql import *
from pyspark.sql.types import *

temp = Row("DESC", "ID")
temp1 = temp('Description1323', 123)

print(temp1)

schema = StructType([StructField("DESC", StringType(), False),
                     StructField("ID", IntegerType(), False)])

# df = my_spark.createDataFrame([temp1], schema)

# df.show()
