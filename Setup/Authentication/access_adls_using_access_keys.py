# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formulaoneracing1dl_account_key = dbutils.secrets.get(scope = 'formulaoneracing1-scope', key = 'formulaoneracing1dl-account-key')

# COMMAND ----------

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formulaoneracing1dl.dfs.core.windows.net",
    formulaoneracing1dl_account_key)

# COMMAND ----------

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulaoneracing1dl.dfs.core.windows.net"))

# COMMAND ----------

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulaoneracing1dl.dfs.core.windows.net/circuits.csv"))