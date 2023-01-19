# Databricks notebook source
import pandas as pd
from sklearn import datasets

# COMMAND ----------

housing_dset = datasets.fetch_california_housing(as_frame=True)
df = pd.concat([housing_dset['data'], housing_dset['target']], axis=1).sample(frac=1.0, random_state=24).reset_index()

# COMMAND ----------

df1 = df.iloc[:6000]
df2 = df.iloc[6000:12000]
df3 = df.iloc[12000:]

# COMMAND ----------

n = 1
for d in [df1, df2, df3]:
    spark.createDataFrame(d).write.saveAsTable(f'housing_data_{n}', mode='overwrite', overwriteSchema=True)
    n += 1

# COMMAND ----------


