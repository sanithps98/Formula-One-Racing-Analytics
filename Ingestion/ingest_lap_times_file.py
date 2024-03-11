# Databricks notebook source
# COMMAND ----------

formulaoneracing1dl_account_key = dbutils.secrets.get(scope = 'formulaoneracing1-scope', key = 'formulaoneracing1dl-account-key')
spark.conf.set(
    "fs.azure.account.key.formulaoneracing1dl.dfs.core.windows.net",
    formulaoneracing1dl_account_key)

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaoneracing1dl.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1: Read the lap_times file

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/lap_times"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/lap_times")


# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3: Write the output to processed container in delta format

# COMMAND ----------

# final_df.write.mode("overwrite").parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/lap_times")
# display(spark.read.parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/lap_times"))

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers