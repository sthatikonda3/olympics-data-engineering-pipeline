# Databricks notebook source
# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("parquet")\
                .load("abfss://bronze@akhilolmpicsdata.dfs.core.windows.net/athletes")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.fillna({"birth_place":"Unknown","birth_country":"Unknown","residence_place":"Unknown","residence_country":"Unknown"})

# COMMAND ----------

display(df)

# COMMAND ----------

df_filter = df.filter((col('current')==True) & (col('name').isin('GALSTYAN Slavik','HARUTYUNYAN Arsen')))

# COMMAND ----------

df = df.withColumn('height',col('height').cast(FloatType()))\
    .withColumn('weight',col('weight').cast(FloatType()))

# COMMAND ----------

df.display()

# COMMAND ----------

df_sorted = df.sort('height','weight',ascending=[0,1]).filter(col('weight')>0)
df_sorted.display()

# COMMAND ----------

df_replaced = df_sorted.withColumn('nationality',regexp_replace('nationality','United States','US'))

# COMMAND ----------

df_replaced.display()

# COMMAND ----------

df_replaced.createOrReplaceTempView("athletes_table")

# COMMAND ----------

df2 = spark.sql("select code,count(code) from athletes_table group by code")

# COMMAND ----------

# MAGIC %md
# MAGIC in pyspark the code will be 
# MAGIC df.groupby('code').agg(count('code').alias('total_count')).filter(col('total_count')>1)

# COMMAND ----------

df_renamed = df_replaced.withColumnRenamed('code','athleteId')

# COMMAND ----------

df_renamed.display()

# COMMAND ----------

df_renamed = df_renamed.withColumn('occupation',split('occupation',','))
df_renamed.display()

# COMMAND ----------

df_final = df_renamed.select('athleteId',
 'current',
 'name',
 'name_short',
 'name_tv',
 'gender',
 'function',
 'country_code',
 'country',
 'country_long',
 'nationality',
 'nationality_long',
 'nationality_code',
 'height',
 'weight',
 'disciplines',
 'events',
 'birth_date',
 'birth_place',
 'birth_country',
 'residence_place',
 'residence_country')


# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df_final.createOrReplaceTempView('final_table')

# COMMAND ----------



# COMMAND ----------

df_windows = spark.sql("select *,sum(weight) over(partition by nationality order by weight desc)  as cum_weight from final_table") 

# COMMAND ----------

df_final.withColumn('cum_weight',sum("weight").over(Window.partitionBy("nationality").orderBy("height"))).display()

# COMMAND ----------

df_windows.display()

# COMMAND ----------

df_final.write.format("delta")\
    .mode("append")\
    .option("path","abfss://silver@akhilolmpicsdata.dfs.core.windows.net/athletes")\
    .saveAsTable("olympics.silver.athletes")