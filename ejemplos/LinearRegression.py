from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Creamos un vector de características utilizando los atributos
vectorAssembler = VectorAssembler(inputCols=['atributo1', 'atributo2', 'atributo3'], outputCol='features')
df = vectorAssembler.transform(df)

# Dividimos los datos en conjuntos de entrenamiento y prueba
splits = df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

# Entrenamos el modelo de regresión lineal
lr = LinearRegression(featuresCol = 'features', labelCol='label', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)

# Realizamos predicciones en el conjunto de prueba
predictions = lr_model.transform(test_df)
