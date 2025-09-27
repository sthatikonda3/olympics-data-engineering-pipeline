# Databricks notebook source
ListOfFiles = [
    {
        "source_container":"bronze",
        "sink_container":"silver",
        "folder":"events"
    },
    {
        "source_container":"bronze",
        "sink_container":"silver",
        "folder":"coaches"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output", value = ListOfFiles)