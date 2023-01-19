# Databricks notebook source
dbutils.widgets.dropdown('series', '1', ['1','2','3'])
series = dbutils.widgets.get('series')

# COMMAND ----------

# MAGIC %run "./pipelines_conf"

# COMMAND ----------

df = spark.read.table(f'housing_data_{series}')
train, test = df.randomSplit([0.8, 0.2], seed=24)

train.write.saveAsTable(DATA_TABLE, mode='overwrite', overwriteSchema=True)
test.write.saveAsTable(VALIDATION_DATA_TABLE, mode='overwrite', overwriteSchema=True)

# COMMAND ----------


