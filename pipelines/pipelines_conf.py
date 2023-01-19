# Databricks notebook source
TARGET_COL = 'MedHouseVal'
PRED_COL = 'pred'
DATA_TABLE = 'housing_data'
VALIDATION_DATA_TABLE = 'housing_data_test'

MODEL_PARAMS = {
    "bootstrap": True,
    "ccp_alpha": 0.0,
    "criterion": "mse",
    "max_depth": 9,
    "max_features": 4,
    "max_leaf_nodes": None,
    "max_samples": None,
    "min_impurity_decrease": 0.0,
    "min_impurity_split": None,
    "min_samples_leaf": 1,
    "min_samples_split": 2,
    "min_weight_fraction_leaf": 0.0,
    "n_estimators": 700,
    "n_jobs": None,
    "oob_score": False,
    "random_state": None,
    "verbose": 0,
    "warm_start": False,
}

ENV_CONF = {
    "dev": {
        "model_name": "housing_dev"
    },
    "staging": {
        "model_name": "housing",
        "inference_job_output_table": "staging_inference_output",
        "inference_job_output_mode": "overwrite",
        "monitoring_job_output_table": "staging_monitoring_output",
        "monitoring_job_output_mode": "overwrite",
    },
    "prod": {
        "model_name": "housing",
        "inference_job_output_table": "prod_inference_output",
        "inference_job_output_mode": "append",
        "monitoring_job_output_table": "prod_monitoring_output",
        "monitoring_job_output_mode": "overwrite",
    }
}

def get_env(stage):
    return ENV_CONF[stage]
