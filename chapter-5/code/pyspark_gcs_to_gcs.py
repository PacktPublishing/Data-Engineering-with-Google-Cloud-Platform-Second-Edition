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

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('spark_hdfs_to_hdfs') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

BUCKET_NAME = "YOUR-PROJECT-ID-data-bucket"
log_files_rdd = sc.textFile(
    'gs://{}/from-git/chapter-5/dataset/logs_example/*'.format(BUCKET_NAME))

splitted_rdd = log_files_rdd.map(lambda x: x.split(" "))
selected_col_rdd = splitted_rdd.map(lambda x: (x[0], x[3], x[5], x[6]))

columns = ["ip", "date", "method", "url"]
logs_df = selected_col_rdd.toDF(columns)
logs_df.createOrReplaceTempView('logs_df')

sql = """
  SELECT
  url,
  count(*) as count
  FROM logs_df
  WHERE url LIKE '%/article%'
  GROUP BY url
  """
article_count_df = spark.sql(sql)
print(" ### Get only articles and blogs records ### ")
article_count_df.show(5)

article_count_df.write.save(
    'gs://{}/chapter-5/job-result/article_count_df'.format(BUCKET_NAME), format='csv', mode='overwrite')
