import os
import argparse
from job_conf import TrainingPipeline


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stage', required=True)
    args = parser.parse_args()
    
    job = TrainingPipeline(args.stage, os.getenv('DATABRICKS_HOST'), os.getenv('DATABRICKS_TOKEN'))
    job.execute()
    
if __name__ == '__main__':
    main()