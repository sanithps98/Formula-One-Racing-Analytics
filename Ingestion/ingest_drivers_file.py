# Databricks notebook source
# COMMAND ----------

formulaoneracing1dl_account_key = dbutils.secrets.get(scope = 'formulaoneracing1-scope', key = 'formulaoneracing1dl-account-key')
spark.conf.set(
    "fs.azure.account.key.formulaoneracing1dl.dfs.core.windows.net",
    formulaoneracing1dl_account_key)

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formulaoneracing1dl.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read the Drivers file 

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat,lit

# COMMAND ----------

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])


# COMMAND ----------

drivers_df = spark.read.option("header",True)\
    .schema(drivers_schema)\
    .csv("abfss://raw@formulaoneracing1dl.dfs.core.windows.net/drivers.csv")

display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id  
# MAGIC 2. driverRef renamed to driver_ref  
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

drivers_df = drivers_df.withColumn("name", concat(col("forename"), lit(" "), col("surname")))
drivers_df = drivers_df.drop("forename", "surname")
display(drivers_df)



# COMMAND ----------


from pyspark.sql.functions import current_timestamp

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp())

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 4: Write the output to processed container in delta format

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/drivers")
# display(spark.read.parquet("abfss://processed@formulaoneracing1dl.dfs.core.windows.net/drivers"))

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers