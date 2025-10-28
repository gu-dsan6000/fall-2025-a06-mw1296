# Brief description of your approach for each problem

## Table of Contents

- [Problem1-1](#Log--level--counts)
- [Problem1-2](#sample--log--entries)
- [Problem1-3](#summary--statistics)
- [Problem2-1](#time--series--application)
- [Problem2-2](#cluster--summary)
- [Problem2-3](#summary--statistics)
- [Problem2-4](#bar--chart)
- [Problem2-5](#density--plot)
---

## Problem1-1

Count records grouped by the column of level using groupby and count() functions
## Problem1-2

Pull 10 random recoreds using sample() and limit() functions filtering for records that level is not empty

## Problem1-3

First get total rows in the dataset
Then fiilter for rows where level is not empty
countDistinct levels
For the log level distribution, calculate the percentage representation of each log level using the record to divide total record.
Finally, write data above to the text file line by line

## Problem2-1

Extract the application_id and app_number from the file name using regexp
Get the start time and end time using the timestamp column for each application_id
Show the start and end time by cluster_id and application_id

## Problem2-2
Group by cluster_id, find the first executed app and the last executed app

## Problem2-3
Count distinct cluster_id
Count distinct application_id
count applications grouped by cluster_id, then take the average of the counts
Count applications by cluster_id and select top5 with most applications

## Problem2-4
use seaborn to plt the bar chart with cluster as x and number of applications as y
use cluster_id as hue

## Problem2-5
calculate job durations using end time minums start time by cluster
use seaborn to generate histogram with bin size as 2000
set kde =True in order to graph the density line on the same graph
