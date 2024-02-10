# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get('GCP_PROJECT')
REGION = 'asia-southeast2'
CLUSTER_NAME = 'ephemeral-spark-cluster-{{ ds_nodash }}'
PYSPARK_URI = 'gs://wired-apex-392509-data-bucket/from-git/chapter-5/code/pyspark_gcs_to_bq.py'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
                    "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
                    }
}

cluster_config = ClusterGenerator(
    project_id=PROJECT_ID,
    master_machine_type="n1-standard-4",
    num_workers=0,
    idle_delete_ttl=600
).make()

args = {
    'owner': 'packt-developer',
}

with DAG(
    dag_id='dataproc_ephemeral_cluster_job',
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
    default_args=args
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=cluster_config,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )

create_cluster >> pyspark_task >> delete_cluster
