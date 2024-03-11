# Databricks notebook source
# COMMAND ----------

formulaoneracing1dl_account_key = dbutils.secrets.get(scope = 'formulaoneracing1-scope', key = 'formulaoneracing1dl-account-key')
spark.conf.set(
    "fs.azure.account.key.formulaoneracing1dl.dfs.core.windows.net",
    formulaoneracing1dl_account_key)

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaoneracing1dl.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1: Read the races file using the spark dataframe reader API

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
]) 

# COMMAND ----------

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------


# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp())\
                                .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) 

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Select only the columns required & rename as required

# COMMAND ----------

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                              col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))


# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 5 - Write the output to the processed container in delta format

# COMMAND ----------

# races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/races")

# display(spark.read.parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/races"))

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races