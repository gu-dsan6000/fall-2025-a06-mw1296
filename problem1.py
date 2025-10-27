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
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Initialize Spark
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

# Load all log files
logs_df = spark.read.text("spark-cluster/data/raw/application_*/*.log")

# Parse log entries
parsed_logs = logs_df.select(
    regexp_extract(input_file_name(), r'(application_\d+_\d+)', 1).alias('application_id'),
    regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('level'),
    regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
    col('value').alias('message')
)

# ###### Problem1-1 #######
# Log level count
log_level_counts = parsed_logs.groupBy('level').count()
# log_level_counts.show()
# # Save log level count to csv
# log_level_counts.toPandas().to_csv('spark-cluster/problem1_counts.csv')

# ###### Problem1-2 #######
# # 10 random sample log entries with their levels
# sampled_df = parsed_logs.filter(col('level') !='').sample(withReplacement=False, fraction=0.01).limit(10)
# sampled_df.toPandas().to_csv('spark-cluster/problem1_sample.csv')

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
with open('spark-cluster/problem1_summary.txt','w') as f:
  f.write(distribution_text)
