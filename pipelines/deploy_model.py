# Databricks notebook source
dbutils.widgets.dropdown('stage', 'staging', ['staging', 'prod'])
stage = dbutils.widgets.get('stage')

# COMMAND ----------

# MAGIC %run "./pipelines_conf"

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from sklearn.metrics import r2_score

# COMMAND ----------

class RegisteredModel:
    def __init__(self, stage):
        self.stage = stage
        self.model_name = MODEL_NAME
        self.client = MlflowClient()
    
    @staticmethod
    def get_test_data():
        data = spark.read.table(VALIDATION_DATA_TABLE).toPandas().drop('index', axis=1)
        X = data.iloc[:, :-1]
        y = data.iloc[:, -1]
        return X, y

    def get_new_model(self):
        model_ver = self.client.get_latest_versions(self.model_name, stages=['None'])[0]
        model = mlflow.pyfunc.load_model(f'models:/{self.model_name}/{model_ver.version}')
        return model, model_ver

    def get_prod_model(self):
        prod_model_list = [mv for mv in self.client.search_model_versions(filter_string=f"name='{self.model_name}'") if mv.current_stage=='Production']
        prod_model = mlflow.pyfunc.load_model(f'models:/{self.model_name}/Production') if prod_model_list else None
        return prod_model
    
    @staticmethod
    def evaluate(model, X, y):
        pred = model.predict(X)
        r2 = r2_score(y, pred)
        return r2
    
    def transit_model_stage(self, version, promote):
        if promote:
            self.client.transition_model_version_stage(self.model_name, version, 'Production', archive_existing_versions=True)
        else:
            self.client.transition_model_version_stage(self.model_name, version, 'Archived')
            if self.stage == 'staging':
                raise Exception('New model does not outperform Prod model')
        
    def compare(self):
        X, y = self.get_test_data()
        new_model, new_model_ver = self.get_new_model()
        prod_model = self.get_prod_model()

        if prod_model:
            new_r2 = self.evaluate(new_model, X, y)
            try:
                prod_r2 = self.evaluate(prod_model, X, y)
                if new_r2 > prod_r2:
                    self.transit_model_stage(new_model_ver.version, promote=True)
                else:
                    self.transit_model_stage(new_model_ver.version, promote=False)
            except:
                self.transit_model_stage(new_model_ver.version, promote=True)
        else:
            self.transit_model_stage(new_model_ver.version, promote=True)
        


# COMMAND ----------

def main():
    registered_model = RegisteredModel(stage)
    registered_model.compare()

# COMMAND ----------

if __name__ == '__main__':
    main()

# COMMAND ----------



# COMMAND ----------


