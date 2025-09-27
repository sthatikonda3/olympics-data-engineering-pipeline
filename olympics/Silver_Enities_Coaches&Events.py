# Databricks notebook source
# MAGIC %md
# MAGIC ## Dynamic Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC **parameters**

# COMMAND ----------

dbutils.widgets.text("source_container","")
dbutils.widgets.text("sink_container","")
dbutils.widgets.text("folder","")

# COMMAND ----------

source_container = dbutils.widgets.get("source_container")
sink_container = dbutils.widgets.get("sink_container")
folder = dbutils.widgets.get("folder")

# COMMAND ----------

# MAGIC %md
# MAGIC **Parametrizing_Code**

# COMMAND ----------

df = spark.read.format("parquet")\
                    .load(f"abfss://{source_container}@akhilolmpicsdata.dfs.core.windows.net/{folder}")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .option("path",f"abfss://{sink_container}@akhilolmpicsdata.dfs.core.windows.net/{folder}")\
        .saveAsTable(f"olympics.{sink_container}.{folder}")

# COMMAND ----------

df.display()