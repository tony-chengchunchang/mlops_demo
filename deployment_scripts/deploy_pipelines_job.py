import sys
sys.path.append('../')
import os
import argparse
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi
import common

parser = argparse.ArgumentParser()
parser.add_argument('--stage', required=True)
args = parser.parse_args()
env = common.get_env(args.stage)

def init_api_client():
    client = ApiClient(host=os.getenv('DATABRICKS_HOST'), token=os.getenv('DATABRICKS_TOKEN'))
    ws_api = WorkspaceApi(client)
    return ws_api

def deploy_project(ws_api):
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