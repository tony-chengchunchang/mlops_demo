# Databricks notebook source
import pandas as pd
from sklearn import datasets

# COMMAND ----------

housing_dset = datasets.fetch_california_housing(as_frame=True)
df = spark.createDataFrame(pd.concat([housing_dset['data'], housing_dset['target']], axis=1))
df.write.saveAsTable('housing_data', mode='overwrite')

# COMMAND ----------


