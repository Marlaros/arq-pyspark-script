from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear Sesion de Spark
spark = SparkSession.builder.appName("Arquitectura 1").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

#Se lee el archivo

df = spark.read.csv("TGN_Entrega_GBA_Agosto.csv", header="true",sep=";")

df.show(10)

# -- CASOS DE USO --

# 1) Volumen Nominado por mes

from pyspark.sql.functions import to_date, month, avg, sum, col, when, regexp_replace, expr

# Convertir la columna 'fecha' al formato de fecha correcto
df1 = df.withColumn('Fecha', when(expr("substring(Fecha, 5, 1) = '/'"), to_date('Fecha', 'dd/MM/yyyy')).otherwise(to_date('Fecha', 'MM/dd/yyyy')))

# Reemplazar comas en 'Volumen Nominado (miles m3)' y convertir a tipo numérico
df1 = df1.withColumn('Volumen Nominado (miles m3)', regexp_replace('Volumen Nominado (miles m3)', ',', '').cast('double'))

# Reemplazar nulos en 'Volumen Nominado (miles m3)' con 0
df1 = df1.withColumn('Volumen Nominado (miles m3)', when(col('Volumen Nominado (miles m3)').isNull(), 0).otherwise(col('Volumen Nominado (miles m3)')))

# Agregar una columna adicional para identificar valores no numéricos
df1 = df1.withColumn('EsNumerico', when(df1['Volumen Nominado (miles m3)'].cast('string').rlike('^[0-9]+\\.?[0-9]*$'), 1).otherwise(0))

# Filtrar registros donde la columna no es numérica
df1 = df1.filter(df1['EsNumerico'] == 1)

result = df1.groupBy(month('Fecha').alias('mes')).agg(
    avg('Volumen Nominado (miles m3)').alias('volumen_nominado_promedio'),
    sum('Volumen Nominado (miles m3)').alias('volumen_nominado_total')
)

print("Volumen Nominado Total por mes")
result.show()


# 2) Calcular la sumatoria de volúmenes nominados por Punto de Entrega y Gasoducto

result2 = df1.groupBy('Punto de Entrega','Gasoducto').agg(sum('Volumen Nominado (miles m3)').alias('sumatoria_volumen_nominado'))

print("Volumen Nominado Total: Por Gasoducto y Puntos de Entrega")
result2.show(10)


# 3) Calcular la sumatoria de volúmenes autorizados por Punto de Entrega y Gasoducto

# Reemplazar comas en 'volumen nominado' y convertir a tipo numérico
df3 = df1.withColumn('Volumen Autorizado (miles m3)',regexp_replace('Volumen Autorizado (miles m3)', ',', '').cast('double'))

# Reemplazar nulos en 'volumen nominado' con 0
df3 = df3.withColumn('Volumen Autorizado (miles m3)', when(col('Volumen Autorizado (miles m3)').isNull(), 0).otherwise(col('Volumen Autorizado (miles m3)')))

result3 = df3.groupBy('Punto de Entrega','Gasoducto').agg(sum('Volumen Autorizado (miles m3)').alias('sumatoria_volumen_autorizado'))

print("Volumen Autorizado Total: Por Gasoducto y Puntos de Entrega")
result3.show()


# 4) Calcular la cantidad de entregas por cargador

from pyspark.sql.functions import count

result4 = df3.groupBy('Cargadores').agg(count('Fecha').alias('cantidad_entregas'))

print("Cantidad de Entregas por Cargador")
result4.show()
