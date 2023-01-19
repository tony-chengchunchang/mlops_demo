import time
from abc import ABC, abstractmethod
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.runs.api import RunsApi
from databricks_cli.jobs.api import JobsApi

ENV_CONF = {
    "staging": {
        "deploy_pipelines_job_source_path": "pipelines/",
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
        "check_state_freq": 20,
        "job_spark_version": "11.3.x-cpu-ml-scala2.12",
        "job_node_type_id": "Standard_D3_v2",
        "job_num_workers": 2,
    },
    "prod": {
        "deploy_pipelines_job_source_path": "pipelines/",
        "deploy_pipelines_job_target_path": "/prod/pipelines/",
        "get_features_job_name": "get_features",
        "get_feature_job_notebook_path": "/prod/pipelines/get_features",
        "train_model_job_name": "train_model",
        "train_model_job_notebook_path": "/prod/pipelines/train_model",
        "deploy_model_job_name": "deploy_model",
        "deploy_model_job_notebook_path": "/prod/pipelines/deploy_model",
        "inference_job_name": "inference",
        "inference_job_notebook_path": "/prod/pipelines/inference",
        "monitoring_job_name": "monitoring",
        "monitoring_job_notebook_path": "/prod/pipelines/monitoring",
        "job_spark_version": "11.3.x-cpu-ml-scala2.12",
        "job_node_type_id": "Standard_D3_v2",
        "job_num_workers": 2,
    }
}

def get_env(stage):
    return ENV_CONF[stage]

class JobHandler(ABC):
    def __init__(self, stage, host, token):
        self.stage = stage
        self.host = host
        self.token = token
        self.env = get_env(self.stage)
        self.job_name = None
        self.client = ApiClient(host=self.host, token=self.token)
        self.jobs_api = JobsApi(self.client)
        self.runs_api = RunsApi(self.client)
    
    @abstractmethod
    def gen_job_dict(self):
        pass
    
    def run(self):
        res = self.runs_api.submit_run(self.job_dict)
        run_id = res['run_id']
        while True:
            run = self.runs_api.get_run(run_id)
            print(run['state'])
            if run['state']['life_cycle_state'] in ['INTERNAL_ERROR', 'SKIPPED', 'TERMINATED']:
                break
            else:
                time.sleep(self.env['check_state_freq'])
                
        if run['state']['result_state'] != 'SUCCESS':
            raise Exception('job failed')
        
    def get(self):
        return self.jobs_api._list_jobs_by_name(self.job_name)
    
    def create(self):
        self.jobs_api.create_job(self.job_dict)
        
    def update(self, job_id):
        self.jobs_api.reset_job({'job_id': job_id, 'new_settings': self.job_dict})
        
    def execute(self):
        if self.stage == 'staging':
            self.run()
        elif self.stage == 'prod':
            job = self.get()
            if job:
                self.update(job[0]['job_id'])
            else:
                self.create()

class TrainingPipeline(JobHandler):
    def __init__(self, stage, host, token):
        super().__init__(stage, host, token)
        self.job_name = 'training_pipeline'
        self.job_dict = self.gen_job_dict()
        
    def gen_job_dict(self):
        job_dict = {
            'name': self.job_name,
            'run_name': self.job_name,
            'tasks': [
                {
                    'task_key': self.env['get_features_job_name'],
                    "new_cluster": {
                        "spark_version": self.env['job_spark_version'],
                        "node_type_id": self.env['job_node_type_id'],
                        "num_workers": self.env['job_num_workers']
                      },
                    'notebook_task': {
                        'notebook_path': self.env['get_feature_job_notebook_path']
                    }
                },
                {
                    'task_key': self.env['train_model_job_name'],
                    'depends_on': [
                        {
                            'task_key': self.env['get_features_job_name']
                        }
                    ],
                    "new_cluster": {
                        "spark_version": self.env['job_spark_version'],
                        "node_type_id": self.env['job_node_type_id'],
                        "num_workers": self.env['job_num_workers']
                      },
                    'notebook_task': {
                        'notebook_path': self.env['train_model_job_notebook_path'],
                        'base_parameters': {
                            'stage': self.stage
                        }
                    }
                },
                {
                    'task_key': self.env['deploy_model_job_name'],
                    'depends_on': [
                        {
                            'task_key': self.env['train_model_job_name']
                        }
                    ],
                    "new_cluster": {
                        "spark_version": self.env['job_spark_version'],
                        "node_type_id": self.env['job_node_type_id'],
                        "num_workers": self.env['job_num_workers']
                      },
                    'notebook_task': {
                        'notebook_path': self.env['deploy_model_job_notebook_path'],
                        'base_parameters': {
                            'stage': self.stage
                        }
                    }
                }
            ]
        }
        return job_dict

class InferenceJob(JobHandler):
    def __init__(self, stage, host, token):
        super().__init__(stage, host, token)
        self.job_name = self.env['inference_job_name']
        self.job_dict = self.gen_job_dict()
        
    def gen_job_dict(self):
        job_dict = {
            'name': self.job_name,
            'run_name': self.job_name,
            "new_cluster": {
                "spark_version": self.env['job_spark_version'],
                "node_type_id": self.env['job_node_type_id'],
                "num_workers": self.env['job_num_workers']
              },
            'notebook_task': {
                'notebook_path': self.env['inference_job_notebook_path'],
                'base_parameters': {
                    'stage': self.stage,
                    'series': '1'
                }
            }
        }

class MonitoringJob(JobHandler):
    def __init__(self, stage, host, token):
        super().__init__(stage, host, token)
        self.job_name = self.env['monitoring_job_name']
        self.job_dict = self.gen_job_dict()
        
    def gen_job_dict(self):
        job_dict = {
            'name': self.job_name,
            'run_name': self.job_name,
            "new_cluster": {
                "spark_version": self.env['job_spark_version'],
                "node_type_id": self.env['job_node_type_id'],
                "num_workers": self.env['job_num_workers']
              },
            'notebook_task': {
                'notebook_path': self.env['monitoring_job_notebook_path'],
                'base_parameters': {
                    'stage': self.stage, 
                    'series': '1'
                }
            }
        }
    