# Databricks notebook source
print('Hello World')

# COMMAND ----------

import json

# COMMAND ----------

with open('test.json') as f:
    data = json.load(f)

# COMMAND ----------

data

# COMMAND ----------


