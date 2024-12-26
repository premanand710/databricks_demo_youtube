# Databricks notebook source
# MAGIC  %sh
# MAGIC  rm -r /dbfs/FileStore
# MAGIC  mkdir /dbfs/FileStore
# MAGIC  wget -O /dbfs/FileStore/sample_sales.csv https://github.com/MicrosoftLearning/mslearn-databricks/raw/main/data/sample_sales.csv

# COMMAND ----------

 from pyspark.sql.types import DateType, StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Date",DateType()),
    StructField("Product",StringType()),
    StructField("Quantity",IntegerType()),
    StructField("Price",IntegerType())])
 
 df = spark.read.csv("/FileStore/sample_sales.csv",schema=schema,header=True)

 display(df)
 df.printSchema()

# COMMAND ----------

df.schema

# COMMAND ----------

from pyspark.sql.functions import year
df.withColumn("Year", year(df["Date"])).show()
