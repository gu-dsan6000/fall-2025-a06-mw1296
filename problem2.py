#!/usr/bin/env python3

import os
import subprocess
import sys
import shutil
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
regexp_extract, col, min, max, to_timestamp, input_file_name, countDistinct, avg, unix_timestamp
)
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Initialize Spark
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

# Load all log files
logs_df = spark.read.text("spark-cluster/data/raw/application_*/*.log")


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
time_series.toPandas().to_csv('spark-cluster/problem2_timeline.csv')


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
cluster_summary.toPandas().to_csv('spark-cluster/problem2_cluster_summary.csv')


###### Problem2-3 #######
# number of clusters
unique_clusters = parsed_logs.select(countDistinct('cluster_id')).collect()[0][0]
# total applications
total_applications = parsed_logs.select(countDistinct('application_id')).collect()[0][0]
# average applications per cluster
avg_app_per_cluster = cluster_summary.select(avg('num_applications')).collect()[0][0]
# most heavily used clusters
top_clusters = cluster_summary.select('cluster_id','num_applications').orderBy('num_applications', ascending=False).limit(1)
top_clusters_df = []
for row in top_clusters.collect():
    cluster_id = row['cluster_id']
    num_applications = row['num_applications']
    top_clusters_df.append(f'Cluster {cluster_id}: {num_applications} applications')
top_clusters_text = f'Total unique cluxsters: {unique_clusters} \n Total applications: {total_applications} \n Average applications per cluster:{avg_app_per_cluster} \n Most heavily used clusters: \n' + '\n'.join(top_clusters_df)
# save the stats summmary to txft
with open('spark-cluster/problem2_stats.txt','w') as f:
  f.write(top_clusters_text)


###### Problem2-4 #######
# Bar chart
# create bar chart df
cluster_pd = (
  cluster_summary
  .select('cluster_id','num_applications')
  .toPandas()
)
# create bar chart
plt.figure(figsize = (10,8))
ax = sns.barplot(x = 'cluster_id',
                 y='num_applications',
                 data=cluster_pd,
                 hue='cluster_id')
plt.xticks(rotation=45, ha='right')
# add labels
for container in ax.containers:
  ax.bar_label(container, padding=2)
# add title
plt.title('number of applications per cluster')
plt.show()
plt.savefig('spark-cluster/problem2_bar_chart.png')


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

bins = list(range(min_val, max_val+2000, 2000))
# create density plot - without log-transform
sns.histplot(
  data = largest_cluster_pd,
  x = 'duration_seconds',
  bins = bins,
  kde=True,
  color='blue',
  edgecolor='black'
)
# add title including sample count
plt.title(f'Job duration distribution with {len(largest_cluster_pd)} number of applications sample')
plt.show()
plt.savefig('spark-cluster/problem2_density_plot.png')

# create density plot - without log-transform

largest_cluster_pd['log_duration'] = np.log10(largest_cluster_pd['duration_seconds'])
bins = np.arange(largest_cluster_pd['log_duration'].min(),
                 largest_cluster_pd['log_duration'].max() + 0.05, 0.05)
sns.histplot(
  data = largest_cluster_pd,
  x = 'log_duration',
  bins = bins,
  kde=True,
  color='blue',
  edgecolor='black'
)
# add title including sample count
plt.title(f'Job duration distri. with {len(largest_cluster_pd)} number of apps (log-transformed)')
plt.show()
plt.savefig('spark-cluster/problem2_density_plot_log_transformed.png')