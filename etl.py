import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, LongType, DecimalType

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, write_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    song_json_schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), True),
        StructField("year", IntegerType(), True)
    ])

    # read song data file
    df = spark.read.json(song_data, schema=song_json_schema)
    df.show(5)
    df.printSchema()

    # create a view with the raw song data to use SQL to select columns
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql('''
        select song_id, title, artist_id, year, duration
        from song_data_table
    ''')
    songs_table.show()

    # write songs table to parquet files partitioned by year and artist
    if write_data == 1:
        songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs_table")

    # extract columns to create artists table
    artists_table = spark.sql('''
        select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        from song_data_table
    ''')
    artists_table.show()

    # write artists table to parquet files
    if write_data == 1:
        artists_table.write.mode("overwrite").parquet(output_data + "artists_table")


def process_log_data(spark, input_data, output_data, write_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    """
    |-- artist: string (nullable = true)
    |-- auth: string (nullable = true)
    |-- firstName: string (nullable = true)
    |-- gender: string (nullable = true)
    |-- itemInSession: long (nullable = true)
    |-- lastName: string (nullable = true)
    |-- length: double (nullable = true)
    |-- level: string (nullable = true)
    |-- location: string (nullable = true)
    |-- method: string (nullable = true)
    |-- page: string (nullable = true)
    |-- registration: double (nullable = true)
    |-- sessionId: long (nullable = true)
    |-- song: string (nullable = true)
    |-- status: long (nullable = true)
    |-- ts: long (nullable = true)
    |-- userAgent: string (nullable = true)
    |-- userId: string (nullable = true)
    """

    log_json_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", IntegerType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())
    ])

    # read song data file
    df = spark.read.json(log_data, schema=log_json_schema)
    #df = spark.read.json(log_data)
    df.show(5)
    df.printSchema()

    # rename some column names
    df2 = df.withColumnRenamed("firstName", "first_name") \
            .withColumnRenamed("itemInSession","item_in_session") \
            .withColumnRenamed("lastName","last_name") \
            .withColumnRenamed("sessionId","session_id") \
            .withColumnRenamed("userAgent","user_agent") \
            .withColumnRenamed("userId","user_id")
    df2.printSchema()

    # filter by actions for song plays
    df2 = df2.filter(df2.page == 'NextSong')
    df2.show(5)

    # create timestamp column from original timestamp column, for later use with time table
    get_dt = udf(lambda x: datetime.fromtimestamp(x/1000).replace(microsecond=0),TimestampType())
    df2 = df2.withColumn("start_time", get_dt("ts"))
    df2.show(5)

    # create a view with the raw songs data to use SQL to select columns
    df2.createOrReplaceTempView("log_data_table")

    # extract columns for users table
    users_table = spark.sql('''
        select distinct(user_id), first_name, last_name, gender, level
        from log_data_table
    ''')
    users_table.show(5)

    # write users table to parquet files
    if write_data == 1:
        users_table.write.mode("overwrite").parquet(output_data + "users_table")

    # extract columns to create time table
    time_table = spark.sql('''
        select
            start_time,
            hour(start_time) as hour,
            day(start_time) as day,
            weekofyear(start_time) as week,
            month(start_time) as month,
            year(start_time) as year,
            dayofweek(start_time) as weekday
        from log_data_table
    ''')
    time_table.show(5)

    # write time table to parquet files partitioned by year and month
    if write_data == 1:
        time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time_table")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table")
    song_df.show(5)

    song_df.createOrReplaceTempView("song_data_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql('''
        select
            monotonically_increasing_id() as songplay_id,
            ld.start_time,
            ld.user_id,
            ld.level,
            sd.song_id,
            sd.artist_id,
            ld.session_id,
            ld.location,
            ld.user_agent,
            month(ld.start_time) as month,
            year(ld.start_time) as year
        from log_data_table ld
        left join song_data_table sd on ld.song = sd.title
    ''')
    songplays_table.show(10000)

    # write songplays table to parquet files partitioned by year and month
    if write_data == 1:
        songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays_table")


def main():
    # set to 1 to write the data to the output, if 0 then just retrieve and display data
    write_data = 1
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data = "./large_data/"
    output_data = "s3a://ma2516-us-west-2/"
    #output_data = "./output/"

    process_song_data(spark, input_data, output_data, write_data)
    process_log_data(spark, input_data, output_data, write_data)


if __name__ == "__main__":
    main()
