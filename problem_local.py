from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, min, max, to_timestamp, input_file_name, countDistinct, avg, unix_timestamp

# Initialize Spark
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

# Load all log files
logs_df = spark.read.text("data/sample/application_*/*.log")

# Parse log entries
parsed_logs = logs_df.select(
    regexp_extract(input_file_name(), r'(application_\d+_\d+)', 1).alias('application_id'),
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('level'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
    col('value').alias('message')
)

###### Problem1-1 #######
# Log level count
log_level_counts = parsed_logs.groupBy('level').count()
log_level_counts.show()
# Save log level count to csv
log_level_counts.toPandas().to_csv('problem1_counts_local.csv')

###### Problem1-2 #######
# 10 random sample log entries with their levels
sampled_df = parsed_logs.filter(col('level') !='').sample(withReplacement=False, fraction=0.01).limit(10)
sampled_df.toPandas().to_csv('problem1_sample_local.csv')

###### Problem1-3 #######
# Total lines processed
linese_processed = parsed_logs.count()
# Lines with log levels
lines_with_log_levels = parsed_logs.filter(col('level') !='').count()
# Unique log levels found
unique_log_levels_found = parsed_logs.filter(col('level') !='').select('level').distinct().count()
# Log level distribution
total = parsed_logs.count()
distribution = []
for row in log_level_counts.collect():
    level = row['level']
    count = row['count']
    pct = count/total * 100
    distribution.append(f'{level}: {count} ({pct:.2f}%)')
distribution_text = 'Log level distribution: \n' + '\n'.join(distribution)
print(distribution_text)
# save the summmary to csv
with open('problem1_summary_local.txt','w') as f:
  f.write(distribution_text)

###### Problem2-1 #######
# Parse log entries include application, cluster and app columns
parsed_logs = logs_df.select(
    regexp_extract(input_file_name(), r'(application_\d+_\d+)', 1).alias('application_id'),
    regexp_extract(input_file_name(), r'application_(\d+)_\d+', 1).alias('cluster_id'),
    regexp_extract(input_file_name(), r'application_\d+_(\d+)', 1).alias('app_number'),
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('level'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
    col('value').alias('message')
)
# Filter entries where timestamp is not empty
parsed_logs = parsed_logs.filter(col('timestamp') !='')
# Convert the tiemstamp column to timestamp format
parsed_logs = parsed_logs.withColumn(
  'timestamp',
  to_timestamp(col('timestamp'), 'yy/MM/dd HH:mm:ss')
)
# Compute start and end time per application
time_series = (
  parsed_logs
  .groupby('application_id', 'cluster_id','app_number')
  .agg(
    min('timestamp').alias('start_time'),
    max('timestamp').alias('end_time')
  )
  .orderBy('application_id')
)
# Save to csv file
time_series.toPandas().to_csv('problem2_timeline_local.csv')

###### Problem2-2 #######
cluster_summary = (
  parsed_logs
  .groupby('cluster_id')
  .agg(
    countDistinct('application_id').alias('num_applications'),
    min('timestamp').alias('cluster_first_app'),
    max('timestamp').alias('cluster_last_app')
  )
  .orderBy('cluster_id')
)
# save to csv
cluster_summary.toPandas().to_csv('cluster_summary_local.csv')

###### Problem2-3 #######
# number of clusters
unique_clusters = parsed_logs.select(countDistinct('cluster_id')).collect()[0][0]
# total applications
total_applications = parsed_logs.select(countDistinct('application_id'))
# average applications per cluster
avg_app_per_cluster = cluster_summary.select(avg('num_applications')).collect()[0][0]
# most heavily used clusters
top_clusters = cluster_summary.select('cluster_id','num_applications').orderBy('num_applications', ascending=False).limit(5)
top_clusters_df = []
for row in top_clusters.collect():
    cluster_id = row['cluster_id']
    num_applications = row['num_applications']
    top_clusters_df.append(f'Cluster {cluster_id}: {num_applications} applications')
top_clusters_text = 'Most heavily used clusters: \n' + '\n'.join(top_clusters_df)
# save the stats summmary to txft
with open('problem2_stats_local.txt','w') as f:
  f.write(top_clusters_text)

###### Problem2-4 #######
# Bar chart
import seaborn as sns
import matplotlib.pyplot as plt
# create bar chart df
cluster_pd = (
  cluster_summary
  .select('cluster_id','num_applications')
  .toPandas()
)
# create bar chart
ax = sns.barplot(x = 'cluster_id',
                 y='num_applications',
                 data=cluster_pd,
                 hue='cluster_id')
# add labels
for container in ax.containers:
  ax.bar_label(container, padding=2)
# add title
plt.title('number of applications per cluster')
plt.show()


###### Problem2-5 #######
# Density plot
# create largest cluster dataframe
largest_cluster_id = cluster_summary.orderBy('num_applications', ascending=False).limit(1).collect()[0][0]
largest_cluster = time_series.withColumn(
  'duration_seconds',
  unix_timestamp('end_time') - unix_timestamp('start_time')
)
largest_cluster_pd = (
  largest_cluster
  .filter(col('cluster_id')==largest_cluster_id)
  .select('cluster_id','app_number','duration_seconds')
  .toPandas()
)
# create the plot
# create the bins
min_val = largest_cluster_pd['duration_seconds'].min()
max_val = largest_cluster_pd['duration_seconds'].max()
bins = range(min_val, max_val + 20 , 20)
sns.histplot(
  data = largest_cluster_pd,
  x = 'duration_seconds',
  bins = bins,
  kde=True,
  color='blue',
  edgecolor='black',
  log_scale = (True, False)
)
# add title including sample count
plt.title(f'Job duration distribution with {len(largest_cluster_pd)} number of application sample')
plt.show()
