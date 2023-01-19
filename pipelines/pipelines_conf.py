TARGET_COL = 'MedHouseVal'
PRED_COL = 'pred'
DATA_TABLE = 'housing_data'
VALIDATION_DATA_TABLE = 'housing_data_test'

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