# Databricks notebook source
import mlflow
from mlflow.tracking import MlflowClient
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from hyperopt import fmin, tpe, hp, SparkTrials, STATUS_OK
from hyperopt.pyll import scope

# COMMAND ----------

def get_dataset():
    return spark.read.table('housing_data').toPandas()

def create_train_test_split(data):
    X = data.iloc[:, :-1]
    y = data.iloc[:, -1]
    X_train, X_test, y_train, y_test = train_test_split(X, y,test_size=0.2, random_state=24)
    return X_train, X_test, y_train, y_test

def create_training_pipeline(**params):
    preproccesor = ColumnTransformer(
        [('scaler', MinMaxScaler(), make_column_selector(dtype_exclude='object'))],
        remainder='passthrough'
    )
    estimator = RandomForestRegressor(**params)
    pipeline = Pipeline(
        [('prep', preproccesor), ('model', estimator)]
    )
    return pipeline

def run():
    with mlflow.start_run() as run:
        data = get_dataset()
        X_train, X_test, y_train, y_test = create_train_test_split(data)
        model = create_training_pipeline(**params)
        model.fit(X_train, y_train)

        pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, pred)
        mse = mean_squared_error(y_test, pred)
        r2 = r2_score(y_test, pred)

        mlflow.log_metrics({'mae':mae, 'mse':mse, 'r2':r2, 'loss': -r2})

if __name__ == '__main__':
    run()

# COMMAND ----------

# with_tuning
# model_name

# COMMAND ----------


