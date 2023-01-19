# Databricks notebook source
import argparse
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi
import job_conf

# COMMAND ----------

# parser = argparse.ArgumentParser()
# parser.add_argument('--stage', required=True)
# args = parser.parse_args()
# env = common.get_env(args.stage)
env = {
        "deploy_pipelines_job_source_path": "../pipelines/",
        "deploy_pipelines_job_target_path": "/staging/pipelines/",
        "get_features_job_name": "get_features",
        "get_feature_job_notebook_path": "/staging/pipelines/get_features",
        "train_model_job_name": "train_model",
        "train_model_job_notebook_path": "/staging/pipelines/train_model",
        "deploy_model_job_name": "deploy_model",
        "deploy_model_job_notebook_path": "/staging/pipelines/deploy_model",
        "inference_job_name": "inference",
        "inference_job_notebook_path": "/staging/pipelines/inference",
        "monitoring_job_name": "monitoring",
        "monitoring_job_notebook_path": "/staging/pipelines/monitoring",
        "job_spark_version": "11.3.x-cpu-ml-scala2.12",
        "job_node_type_id": "Standard_D3_v2",
        "job_num_workers": 2,
    }
def init_api_client():
    client = ApiClient(host=os.getenv('DATABRICKS_HOST'), token=os.getenv('DATABRICKS_TOKEN'))
    ws_api = WorkspaceApi(client)
    return ws_api

def deploy_project(ws_api):
    ws_api.import_workspace(
        source_path='../common.py',
        target_path='/tmp/test_text',
        language='PYTHON',
        fmt='SOURCE',
        is_overwrite=True
    )
    ws_api.import_workspace_dir(
        source_path=env['deploy_pipelines_job_source_path'],
        target_path=env['deploy_pipelines_job_target_path'],
        overwrite=True,
        exclude_hidden_files=False
    )
    
def main():
    ws_api = init_api_client()
    deploy_project(ws_api)
    
if __name__ == '__main__':
    main()
