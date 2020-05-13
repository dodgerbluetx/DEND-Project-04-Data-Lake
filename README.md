# Data Engineering Nanodegree
## Project 04 - Data Lakes

## Introduction

A music streaming startup, Sparkify, has grown their user base and song
database even more and want to move their data warehouse to a data lake. Their
data resides in S3, in a directory of JSON logs on user activity on the app, as
well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that
extracts their data from S3, processes them using Spark, and loads the data
back into S3 as a set of dimensional tables. This will allow their analytics
team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given
to you by the analytics team from Sparkify and compare your results with their
expected results.

## Project Description

In this project, you'll apply what you've learned on Spark and data lakes to
build an ETL pipeline for a data lake hosted on S3. To complete the project,
you will need to load data from S3, process the data into analytics tables
using Spark, and load them back into S3. You'll deploy this Spark process on a
cluster using AWS.

## Requirements

The ETL script can be run locally against a data set, or remote in an EMR
cluster. Use the instructions below to set up the environment for each method.

The `./data` directory included in the repo contains a small sample of the
Sparkify dataset.


## Job Execution

#### Local Method

Ensure that your environment is running Python 3.7 or greater and PySpark 2.4.5

1. Modify the following code in the etl.py script to point the input/output
data locations to local directories:

  ~~~~
  input_data = "./data/"
  output_data = "./output/"
  ~~~~

2. Run the script using the following command: `python etl.py`

#### Remote EMR Method

An AWS EMR Cluster is required to run the remote method.  Ensure that the
cluster is available and the proper security groups allow access from EMR.  An
S3 bucket is also required for output data, which is specified below.

1. Create an EMR Cluster with the following parameters:

  ~~~~
  Release:        emr-5.29.0
  Applications:   Spark 2.4.4, Hadoop 2.8.5 with YARN, Ganglia 3.7.2 and Zeppelin        0.8.2
  Instance Type:  m5.xlarge (3 Node)
  ~~~~

2. From within the EMR cluster, clone the git repo:

  ~~~~
  git clone https://github.com/dodgerbluetx/DEND-Project-03-DWH_Warehouse.git
  ~~~~

3. Modify the following code in the etl.py script to use the Udacity S3 bucket
for input data and your own S3 bucket for output data.  Ensure that the output
S3 bucket is in the us-west-2 region for better performance.

  ~~~~
  input_data = "s3a://udacity-dend/"
  output_data = "s3a://[bucket name]/"
  ~~~~

4. Create a dl.cfg file in the same directory as the etl.py script, and add
your key information for access to the output S3 bucket.

  ~~~~
  [AWS]
  AWS_ACCESS_KEY_ID = [key]
  AWS_SECRET_ACCESS_KEY = [secret key]
  ~~~~

5. Run the job on the EMR cluster:

  ~~~~
  /usr/bin/spark-submit --master yarn ./etl.py
  ~~~~

## Data Creation

The Sparkify dataset is either read locally or remote from the Udacity S3
bucket. In either case, the song data and log data are both parsed, loaded into
spark, processed and written to parquet files in the output location.

The song and log data are combined to output data to the following tables:

#### Fact Table

  1. `songplays` - records in log data associated with song plays i.e. records with page `NextSong`
    * _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

#### Dimension Tables

  2. `users` - users in the app
    * _user_id, first_name, last_name, gender, level_


  3. `songs` - songs in music database
    * _song_id, title, artist_id, year, duration_


  4. `artists` - artists in music database
    * _artist_id, name, location, lattitude, longitude_


  5. `time` - timestamps of records in songplays broken down into specific units
    * _start_time, hour, day, week, month, year, weekday_
