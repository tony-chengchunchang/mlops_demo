# Databricks notebook source
dbutils.widgets.dropdown('stage', 'staging', ['staging', 'prod'])
stage = dbutils.widgets.get('stage')

dbutils.widgets.dropdown('series', '1', ['1','2','3'])
series = dbutils.widgets.get('series')

# COMMAND ----------

# MAGIC %run "./pipelines_conf"

# COMMAND ----------

import mlflow
import pandas as pd

# COMMAND ----------

class InferenceModel:
    def __init__(self, stage, series):
        self.stage = stage
        self.series = series
        self.env = get_env(self.stage)
        self.model_name = self.env['model_name']
        self.output_table = self.env['inference_job_output_table']
        self.output_mode = self.env['inference_job_output_mode']
        self.pred_col = PRED_COL
        self.data = None
        self.model = None
        self.setup()
    
    def setup(self):
        if self.stage == 'staging':
            self.data = spark.read.table(VALIDATION_DATA_TABLE).toPandas().iloc[:, :-1]
            self.model = mlflow.pyfunc.load_model(f'models:/{self.model_name}/Staging')
        elif self.stage == 'prod':
            self.data = spark.read.table(f'housing_data_{self.series}').toPandas().iloc[:, :-1]
            self.model = mlflow.pyfunc.load_model(f'models:/{self.model_name}/Production')
    
    def predict(self):
        return self.model.predict(self.data)
    
    def execute(self):
        pred = self.predict()
        df = pd.concat([self.data['index'], pd.Series(pred, name=self.pred_col)], axis=1)
        if self.stage == 'prod':
            df['series'] = self.series
        spark.createDataFrame(df).write.saveAsTable(self.output_table, mode=self.output_mode)

# COMMAND ----------

def main():
    inf_model = InferenceModel(stage, series)
    inf_model.execute()

# COMMAND ----------

if __name__ == '__main__':
    main()

# COMMAND ----------


