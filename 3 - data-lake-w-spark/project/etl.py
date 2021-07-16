import configparser
from datetime import datetime
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, to_date, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Extracting song and artist data from the song dataset stored into the bronze layer (raw data).
    
    After extracting all data from the dataset, this module produce the data to the silver layer (refined tables).
    
    Parameters:
    
    spark -> Spark Session,
    input_data -> path to the bronze layer (raw data) from the S3 bucket
    ouput_data -> path to the silver layer (refined tables) from the S3 bucket
    
    Returns: None
    
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_path = os.path.join(output_data, 'songs')
    songs_table.write.parquet(songs_path, mode='overwrite', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id",
                                   "artist_name",
                                   "coalesce(nullif(artist_location, ''), 'N/A') as location",
                                   "coalesce(artist_latitude, 0.0) as latitude",
                                   "coalesce(artist_longitude, 0.0) as longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_path = os.path.join(output_data, 'artists')
    artists_table.write.parquet(artists_path, mode='overwrite')

def process_log_data(spark, input_data, output_data):
    """Extracting users, time and songplays data from the logs dataset stored into the bronze layer (raw data).
    
    After extracting all data from the dataset, this module produce the data to the silver layer (refined tables).
    
    Parameters:
    
    spark -> Spark Session,
    input_data -> path to the bronze layer (raw data) from the S3 bucket
    ouput_data -> path to the silver layer (refined tables) from the S3 bucket
    
    Returns: None
    
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(["userId as user_id", 
                                 "firstName as first_name", 
                                 "lastName as last_name", 
                                 "gender", 
                                 "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_path = os.path.join(output_data, 'users')
    users_table.write.parquet(users_path, mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn("log_timestamp", to_timestamp(df.ts/1000))
    
    # create datetime column from original timestamp column
    df = df.withColumn("log_datetime", to_date(df.log_timestamp))
    
    # extract columns to create time table
    time_table = df.selectExpr(["log_timestamp as start_time", 
                                "hour(log_datetime) as hour", 
                                "dayofmonth(log_datetime) as day", 
                                "weekofyear(log_datetime) as week", 
                                "month(log_datetime) as month", 
                                "year(log_datetime) as year", 
                                "dayofweek(log_datetime) as weekday"]).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_path = os.path.join(output_data, 'time')
    time_table.write.parquet(time_path, mode='append', partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    songs_path = os.path.join(output_data, 'songs')
    song_df = spark.read.parquet(songs_path)
    song_df = song_df.withColumnRenamed("artist_id", "songs_artist_id")
    
    artists_path = os.path.join(output_data, 'artists')
    artists_df = spark.read.parquet(artists_path)
    artists_df = artists_df.withColumnRenamed("artist_id", "artists_artist_id").withColumnRenamed("location", "artist_location")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(df.log_timestamp.alias("start_time"), 
                                 df.userId.alias("user_id"), 
                                 "level", 
                                 "song", 
                                 "artist", 
                                 df.sessionId.alias("session_id"), 
                                 "location", 
                                 df.userAgent.alias("user_agent")) \
                        .join(song_df, df.song==song_df.title, 'left_outer')   \
                        .join(artists_df, df.artist==artists_df.artist_name, 'left_outer') \
                        .selectExpr("start_time",
                                    "user_id",
                                    "level",
                                    "song_id",
                                    "coalesce(artists_artist_id, songs_artist_id) as artist_id",
                                    "session_id",
                                    "location",
                                    "user_agent",
                                    "year(start_time) as year",
                                    "month(start_time) as month") \
                        .dropDuplicates() \
                        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_path = os.path.join(output_data, 'songplays')
    songplays_table.write.parquet(songplays_path, mode='overwrite', partitionBy=["year", "month"])

def main():
    start_time = time.time()
    spark = create_spark_session()
    input_data = "s3a://natanascimento-bronze"
    output_data = "s3a://natanascimento-silver"
    
    process_song_data(spark, input_data, output_data)   
    
    process_log_data(spark, input_data, output_data)
    
    print(f"Execution time: {time.time() - start_time} seconds")

if __name__ == "__main__":
    main()
