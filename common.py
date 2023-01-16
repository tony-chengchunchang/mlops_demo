import json

ENV_CONF = {
    "dev": {
        "model_name": "housing_dev"
    },
    "staging": {
        "model_name": "housing",
        "deploy_pipelines_job_source_path": "pipelines/",
        "deploy_pipelines_job_target_path": "/staging/pipelines/",
        "train_model_job_name": "train_model",
        "train_model_job_spark_version": "11.3.x-cpu-ml-scala2.12",
        "train_model_job_node_type_id": "Standard_D3_v2",
        "train_model_job_num_workers": 2,
        "train_model_job_notebook_path": "/staging/pipelines/train_model",
    },
    "prod": {
        "model_name": "housing",
        "deploy_pipelines_job_source_path": "pipelines/",
        "deploy_pipelines_job_target_path": "/prod/pipelines/",
        "train_model_job_name": "train_model",
        "train_model_job_spark_version": "11.3.x-cpu-ml-scala2.12",
        "train_model_job_node_type_id": "Standard_D3_v2",
        "train_model_job_num_workers": 3,
        "train_model_job_notebook_path": "/prod/pipelines/train_model"
    }
}

def get_env(stage):
    return ENV_CONF[stage]
