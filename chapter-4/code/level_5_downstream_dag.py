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
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator, BigQueryInsertJobOperator)

args = {
    'owner': 'packt-developer',
}

# Environment Variables
gcp_project_id = os.environ.get('GCP_PROJECT')

# Airflow Variables
settings = Variable.get("level_3_dag_settings", deserialize_json=True)
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_dwh_dataset = settings['bq_dwh_dataset']

# DAG Variables
bq_datamart_dataset = 'dm_bikesharing'
bq_fact_trips_daily_table_id = f'{gcp_project_id}.{bq_dwh_dataset}.facts_trips_daily_partition'
sum_total_trips_table_id = f'{gcp_project_id}.{bq_datamart_dataset}.sum_total_trips_daily'

# Macros
logical_date_nodash = '{{ ds_nodash }}'
logical_date = '{{ ds }}'


with DAG(
    dag_id='level_5_downstream_dag',
    default_args=args,
    schedule=[
        Dataset("dwh_fact_trips_daily"),
        Dataset("dwh_dim_stations")
    ],
    start_date=datetime(2018, 1, 1)
) as dag:

    data_mart_sum_total_trips = BigQueryInsertJobOperator(
        task_id="data_mart_sum_total_trips",
        configuration={
            "query": {
                "query": f"""SELECT trip_date,
                                      SUM(total_trips) sum_total_trips
                                      FROM `{bq_fact_trips_daily_table_id}` 
                                      GROUP BY trip_date""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": gcp_project_id,
                    "datasetId": bq_datamart_dataset,
                    "tableId": "sum_total_trips_daily"
                },
                "timePartitioning": {
                    'type': 'DAY',
                    'field': 'trip_date'},
                "createDisposition": 'CREATE_IF_NEEDED',
                "writeDisposition": 'WRITE_TRUNCATE',
                "priority": "BATCH",
            }
        }
    )

    bq_row_count_check_data_mart_sum_total_trips = BigQueryCheckOperator(
        task_id='bq_row_count_check_data_mart_sum_total_trips',
        sql=f"""
        select count(*) from `{sum_total_trips_table_id}`
        """,
        use_legacy_sql=False,
        outlets=[Dataset("data_mart_sum_total_trips")]
    )

    data_mart_sum_total_trips >> bq_row_count_check_data_mart_sum_total_trips

if __name__ == "__main__":
    dag.cli()
