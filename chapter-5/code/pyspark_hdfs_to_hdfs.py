from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName('spark_hdfs_to_hdfs') \
.getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

MASTER_NODE_INSTANCE_NAME="cluster-2a0f-deb10-n14-m"
log_files_rdd = sc.textFile('hdfs://{}/data/logs_example/*'.format(MASTER_NODE_INSTANCE_NAME))

splitted_rdd = log_files_rdd.map(lambda x: x.split(" "))
selected_col_rdd = splitted_rdd.map(lambda x: (x[0], x[3], x[5], x[6]))

columns = ["ip","date","method","url"]
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

article_count_df.write.save('hdfs://{}/data/article_count_df'.format(MASTER_NODE_INSTANCE_NAME), format='csv', mode='overwrite')
