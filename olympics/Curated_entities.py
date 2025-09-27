# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Live Tables - Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC **coaches DLT Pipeline**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expectations for Data Quality

# COMMAND ----------

expec_coaches = {
        "rule1" : "code is not null",
        "rule2" : "current is True",
    }

# COMMAND ----------

expec_nocs ={
    "rule1" : "code is not null",
}

# COMMAND ----------

expec_events ={
    "rule1":"event is not null"
}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table
def source_table_coaches():
    df = spark.readStream.table("olympics.silver.coaches")
    return df 

# COMMAND ----------

@dlt.view

def view_coaches():
    df = spark.readStream.table("LIVE.source_table_coaches")
    df.fillna("Unknown")
    return df 

# COMMAND ----------

@dlt.table
@dlt.expect_all(expec_coaches)
def coaches():
    df = spark.readStream.table("Live.view_coaches")
    return df 

# COMMAND ----------

# MAGIC %md
# MAGIC ## NOCS DLT PIPELINE

# COMMAND ----------

@dlt.table
def source_table_nocs():
    df = spark.readStream.table("olympics.silver.nocs")
    return df 

# COMMAND ----------

@dlt.view

def view_nocs():
    df = spark.readStream.table("LIVE.source_table_nocs")
    df = df.fillna("Unknown")
    return df 

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(expec_nocs)
def nocs():
    df = spark.readStream.table("Live.view_nocs")
    return df 

# COMMAND ----------

# MAGIC %md
# MAGIC ## events DLT pipeline

# COMMAND ----------

@dlt.table
def source_table_events():
    df = spark.readStream.table("olympics.silver.events")
    return df 

# COMMAND ----------

@dlt.view

def view_events():
    df = spark.readStream.table("LIVE.source_table_events")
    df = df.fillna("Unknown")
    return df 

# COMMAND ----------

@dlt.table
@dlt.expect_all(expec_events)
def events():
    df = spark.readStream.table("Live.view_events")
    return df 

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC - APPLY CHANGES

# COMMAND ----------

@dlt.view

def source_athletes():
    df = spark.readStream.table("olympics.silver.athletes")
    return df

# COMMAND ----------

dlt.create_streaming_table("athletes")

# COMMAND ----------

dlt.apply_changes(
    target = "athletes",
    source = "source_athletes",
    keys = ["athleteId"],
    sequence_by = col("height"),
    stored_as_scd_type = 1
)