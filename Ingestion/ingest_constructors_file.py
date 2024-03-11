# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1: Read the constructors file 

# COMMAND ----------

# COMMAND ----------

formulaoneracing1dl_account_key = dbutils.secrets.get(scope = 'formulaoneracing1-scope', key = 'formulaoneracing1dl-account-key')
spark.conf.set(
    "fs.azure.account.key.formulaoneracing1dl.dfs.core.windows.net",
    formulaoneracing1dl_account_key)

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaoneracing1dl.dfs.core.windows.net"))

# COMMAND ----------

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# COMMAND ----------

constructor_df = spark.read.option("header",True) \
.schema(constructors_schema) \
.csv("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/constructors.csv")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2: Drop unwanted columns from the dataframe

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Rename columns and add ingestion date

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------


# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumn("ingestion_date", current_timestamp())



# COMMAND ----------

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 5: Write the output to the processed container in delta format

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/constructors")

# display(spark.read.parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/constructors"))

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors