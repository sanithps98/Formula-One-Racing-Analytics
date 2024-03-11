# Databricks notebook source
# COMMAND ----------

formulaoneracing1dl_account_key = dbutils.secrets.get(scope = 'formulaoneracing1-scope', key = 'formulaoneracing1dl-account-key')
spark.conf.set(
    "fs.azure.account.key.formulaoneracing1dl.dfs.core.windows.net",
    formulaoneracing1dl_account_key)

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaoneracing1dl.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1: Read the qualifying file

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/qualifying"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])


# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("header", True).option("multiLine", True) \
.csv("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3: Write the output to processed container in delta format

# COMMAND ----------

# final_df.write.mode("overwrite").parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/qualifying")
# display(spark.read.parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/qualifying"))

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying