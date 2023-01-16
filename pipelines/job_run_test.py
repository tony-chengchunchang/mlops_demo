# Databricks notebook source
import sys
sys.path.append('../')
import os
import argparse
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.runs.api import RunsApi
from databricks_cli.jobs.api import JobsApi
import common

# COMMAND ----------

token = 'dapi556f78773d703620eea9f03673f31abe'
host = 'https://adb-2594463552369592.12.azuredatabricks.net'

# COMMAND ----------

class JobHandler:
    def __init__(self, stage, host, token):
        self.stage = stage
        self.host = host
        self.token = token
        self.env = common.get_env(self.stage)
        self.job_name = self.env['train_model_job_name']
        self.job_dict = self.gen_job_dict()
        self.client = ApiClient(host=self.host, token=self.token)
        self.jobs_api = JobsApi(self.client)
        self.runs_api = RunsApi(self.client)
    
    def gen_job_dict(self):
        job_dict = {
            'name': self.env['train_model_job_name'],
            'run_name': self.env['train_model_job_name'],
            "new_cluster": {
                "spark_version": self.env['train_model_job_spark_version'],
                "node_type_id": self.env['train_model_job_node_type_id'],
                "num_workers": self.env['train_model_job_num_workers']
              },
            'notebook_task': {
                'notebook_path': self.env['train_model_job_notebook_path'],
                'base_parameters': {
                    'stage': self.stage
                }
            }
        }
        return job_dict
    
    def run_job(self):
        self.runs_api.submit_run(self.job_dict)
        
    def get_job(self):
        return self.jobs_api._list_jobs_by_name(self.job_name)
    
    def create_job(self):
        self.jobs_api.create_job(self.job_dict)
        
    def update_job(self, job_id):
        self.jobs_api.reset_job({'job_id': job_id, 'new_settings': self.job_dict})
        
    def execute(self):
        if self.stage == 'staging':
            self.run_job()
        elif self.stage == 'prod':
            job = self.get_job()
            if job:
                self.update_job(job[0]['job_id'])
            else:
                self.create_job()

# COMMAND ----------

handler = JobHandler('staging', host, token)

# COMMAND ----------

os.path.abspath('../')

# COMMAND ----------



# COMMAND ----------


