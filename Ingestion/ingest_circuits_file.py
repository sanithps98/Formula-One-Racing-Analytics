# Databricks notebook source
# COMMAND ----------

formulaoneracing1dl_account_key = dbutils.secrets.get(scope = 'formulaoneracing1-scope', key = 'formulaoneracing1dl-account-key')
spark.conf.set(
    "fs.azure.account.key.formulaoneracing1dl.dfs.core.windows.net",
    formulaoneracing1dl_account_key)

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaoneracing1dl.dfs.core.windows.net"))

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Includes_configs_and_commonfunctions/configuration"

# COMMAND ----------

# MAGIC %run "../Includes_configs_and_commonfunctions/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1: Read the circuits file

# COMMAND ----------

circuits_df = spark.read.option("header",True).csv("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

#structType is to represent rows
#structField is to represent column (name, datatype, nullable)

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])


# COMMAND ----------

circuits_df = spark.read.option("header",True)\
    .schema(circuits_schema)\
    .csv("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2: Select only the required columns

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Rename the columns as required

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Write data to datalake as delta

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/circuits")
# display(spark.read.parquet(f"{processed_folder_path}/circuits"))

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits