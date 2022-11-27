"""
File to read and process data and load them in parquet files.

The file reads and processes data from the `song_data`
and `log_data` folders in s3 and load them into parquet files.
"""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col, broadcast
from pyspark.sql.functions import year, month, dayofmonth, \
    hour, weekofyear, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')
# set aws configs
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session with hadoop aws.

    :return: SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data.

    Read song data from json logs stored in s3.
    Write `songs` and `artists` dimension tables to parquet
    files, using PySpark.

    :param spark: SparkSession object
    :param input_data: s3 file path for input data
    :param output_data: s3 file path for output data
    """
    # get file paths to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'

    # read song data file
    dataframe = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = (dataframe
                   .select('song_id', 'title', 'artist_id',
                           'artist_name', 'year', 'duration')
                   .dropDuplicates())

    # write songs table to parquet files partitioned by year and artist
    (songs_table
     .write
     .partitionBy("year", "artist_id")
     .mode("overwrite")
     .parquet(output_data + "/songs.parquet"))

    # extract columns to create artists table
    artists_table = (dataframe
                     .selectExpr('artist_id',
                                 'artist_name AS name',
                                 'artist_location AS location',
                                 'artist_latitude AS latitude',
                                 'artist_longitude AS longitude')
                     .dropDuplicates())

    # write artists table to parquet files
    (artists_table
     .write
     .mode("overwrite")
     .parquet(output_data + "/artists.parquet"))


def process_log_data(spark, input_data, output_data):
    """
    Process log data.

    Read log data from json logs stored in s3.
    Write `users` and `time` dimension tables to parquet files, using PySpark.
    Write `songplays` fact table to parquet file, using PySpark.

    :param spark: SparkSession object
    :param input_data: s3 file path for input data
    :param output_data: s3 file path for output data
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*.json'

    # read log data file
    dataframe = spark.read.json(log_data)

    # filter by actions for song plays
    dataframe = dataframe.where("page = 'NextSong'")

    # extract columns for users table
    users_table = (dataframe
                   .selectExpr('userId AS user_id',
                               'firstName AS first_name',
                               'lastName AS last_name',
                               'gender',
                               'level')
                   .dropDuplicates())

    # write users table to parquet files
    (users_table
     .write
     .mode("overwrite")
     .parquet(output_data + "/users.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000),
                        TimestampType())
    dataframe = dataframe.withColumn("start_time", get_timestamp("ts"))
    dataframe = (dataframe
                 .withColumn("hour", hour("start_time"))
                 .withColumn("day", dayofmonth("start_time"))
                 .withColumn("week", weekofyear("start_time"))
                 .withColumn("month", month("start_time"))
                 .withColumn("year", year("start_time"))
                 .withColumn("weekday", dayofweek("start_time")))

    # extract columns to create time table
    time_table = (dataframe
                  .select("start_time", "hour", "day",
                          "week", "month", "year", "weekday")
                  .dropDuplicates())

    # write time table to parquet files partitioned by year and month
    (time_table
     .write
     .partitionBy("year", "month")
     .mode("overwrite")
     .parquet(output_data + "/time.parquet"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs.parquet")

    # extract columns from joined song and log data to create songplays table
    dataframe = dataframe.alias("df")
    song_df = song_df.alias("song")

    songplays_table = (dataframe
                       .join(broadcast(song_df),
                             ((col('df.song') == col('song.title')) &
                              (col('df.artist') == col('song.artist_name')) &
                              (col('df.length') == col('song.duration'))),
                             "left")
                       .selectExpr("df.start_time AS start_time",
                                   "df.userId AS user_id",
                                   "df.level AS level",
                                   "song.song_id AS song_id",
                                   "song.artist_id AS artist_id",
                                   "df.sessionId AS session_id",
                                   "df.location AS location",
                                   "df.userAgent AS user_agent",
                                   "df.year AS year",
                                   "df.month AS month"))

    # write songplays table to parquet files partitioned by year and month
    (songplays_table
     .write
     .partitionBy("year", "month")
     .mode("overwrite")
     .parquet(output_data + "/songplays.parquet"))


def main():
    """Create spark session and execute functions to process data."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-davg/sparkifydb/"
    # process song and log data
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    # stop spark session
    spark.stop()


if __name__ == "__main__":
    main()
