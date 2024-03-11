# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formulaoneracing1dl_demo_sas_token = "sp=rl&st=2024-03-05T17:11:29Z&se=2024-03-06T01:11:29Z&spr=https&sv=2022-11-02&sr=c&sig=pTp1ySUOSlx93A9JQJ%2BJmubPuaXdWUYAStpvLyzeVFk%3D"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulaoneracing1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formulaoneracing1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formulaoneracing1dl.dfs.core.windows.net", formulaoneracing1dl_demo_sas_token)

# COMMAND ----------

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulaoneracing1dl.dfs.core.windows.net"))

# COMMAND ----------

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulaoneracing1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC https://formulaoneracing1dl.blob.core.windows.net/demo?sp=rl&st=2024-03-05T17:11:29Z&se=2024-03-06T01:11:29Z&spr=https&sv=2022-11-02&sr=c&sig=pTp1ySUOSlx93A9JQJ%2BJmubPuaXdWUYAStpvLyzeVFk%3D

# COMMAND ----------

# MAGIC %md
# MAGIC https://formulaoneracing1dl.blob.core.windows.net/demo?sv=2023-11-03&st=2024-03-05T18%3A02%3A38Z&se=2024-03-06T18%3A02%3A38Z&sr=c&sp=rl&sig=LdS9dSe%2BGGyIEoto%2FhaR%2BQuT1WbS7V4mg6kmHGsy5HI%3D

# COMMAND ----------

# MAGIC %md
# MAGIC sv=2023-11-03&st=2024-03-05T18%3A02%3A38Z&se=2024-03-06T18%3A02%3A38Z&sr=c&sp=rl&sig=LdS9dSe%2BGGyIEoto%2FhaR%2BQuT1WbS7V4mg6kmHGsy5HI%3D

# COMMAND ----------

?sv=2023-11-03&st=2024-03-05T18%3A02%3A38Z&se=2024-03-06T18%3A02%3A38Z&sr=c&sp=rl&sig=LdS9dSe%2BGGyIEoto%2FhaR%2BQuT1WbS7V4mg6kmHGsy5HI%3D