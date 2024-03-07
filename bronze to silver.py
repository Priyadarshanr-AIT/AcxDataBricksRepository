# Databricks notebook source
input_path ='/mnt/bronze/Sales/Customer/Customer.parquet'

# COMMAND ----------

df=spark.read.format('parquet').load(input_path)


# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format 
from pyspark.sql.types import TimestampType

df=df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))

# COMMAND ----------

display(df)

# COMMAND ----------

table_name =[]

for item in dbutils.fs.ls('mnt/bronze/Sales/'): table_name.append(item.name.split('/')[0])

# COMMAND ----------

table_name

# COMMAND ----------


