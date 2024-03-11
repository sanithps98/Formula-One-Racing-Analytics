# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------


# COMMAND ----------

dbutils.secrets.list(scope = 'formulaoneracing1-scope')

# COMMAND ----------

# COMMAND ----------

dbutils.secrets.get(scope = 'formulaoneracing1-scope', key = 'formulaoneracing1dl-account-key')