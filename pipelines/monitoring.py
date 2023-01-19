# Databricks notebook source
dbutils.widgets.dropdown('stage', 'staging', ['staging', 'prod'])
stage = dbutils.widgets.get('stage')

dbutils.widgets.dropdown('series', '1', ['1','2','3'])
series = dbutils.widgets.get('series')

# COMMAND ----------

# MAGIC %run "./pipelines_conf"

# COMMAND ----------

import datetime
from sklearn.metrics import r2_score
import pyspark.sql.functions as F

# COMMAND ----------

class MonitorModel:
    def __init__(self, stage, series):
        self.stage = stage
        self.series = series
        self.env = get_env(self.stage)
        self.source_table = self.env['inference_job_output_table']
        self.output_table = self.env['monitoring_job_output_table']
        self.output_mode = self.env['monitoring_job_output_mode']
        self.target_col = TARGET_COL
        self.pred_col = PRED_COL
    
    def cal_r2(self):
        if self.stage == 'staging':
            pred = spark.read.table(self.source_table).toPandas()
            y_true = spark.read.table(VALIDATION_DATA_TABLE).select('index', self.target_col).toPandas()
        elif self.stage == 'prod':
            pred = spark.read.table(self.source_table)\
                .filter(F.col('series')==self.series)\
                .distinct()\
                .toPandas()
            y_true = spark.read.table(f'housing_data_{self.series}').select('index', self.target_col).toPandas()
            
        validation = pred.join(y_true.set_index('index'), on='index')
        r2 = r2_score(validation[self.target_col], validation[self.pred_col])
        return float(r2)
        
    def log_status(self):
        r2 = self.cal_r2()
        now = datetime.datetime.now()
        df = spark.createDataFrame([(r2, now)], ['r2', 'ts'])
        df = df.withColumn('ts', F.from_utc_timestamp('ts', 'Asia/Taipei'))
        df.write.saveAsTable(self.output_table, mode=self.output_mode)

# COMMAND ----------

def main():
    monitor = MonitorModel(stage, series)
    monitor.log_status()

# COMMAND ----------

if __name__ == '__main__':
    main()

# COMMAND ----------


