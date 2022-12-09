import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Description: 
        A function tp setup a new Spark sesson
        
    Arguments:
        None

    Returns:
        None
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Description: 
        A function to load song data from an S3 bucket 
        and extract song and artist information.
        The data is then loaded back to S3.
        
    Arguments:
        spark: The spark session.
        input_data: Input path on S3.
        output_data: Target output path on S3

    Returns:
        None
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # song_data = input_data + 'song_data/A/A/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    # https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(os.path.join(output_data, 'tables/songs.parquet'))

    # extract columns to create artists table
    artists_table = artists_table = df.selectExpr('artist_id', 'artist_name as name', \
                                                  'artist_location as location', 'artist_latitude as latitude', \
                                                  'artist_longitude as longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'tables/artists.parquet'))
    
def process_log_data(spark, input_data, output_data):
    
    """
    Description: 
        A function to load log (and some song) data from an S3 bucket 
        and extract user, time and songplay information.
        Data is transformed to meet structural and type requirments. 
        The data is then loaded back to S3.
        
    Arguments:
        spark: The spark session.
        input_data: Input path on S3.
        output_data: Target output path on S3

    Returns:
        None
    """
        
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    # https://sparkbyexamples.com/pyspark/pyspark-where-filter/
    df_log = df.filter(df.page == "NextSong") 

    # extract columns for users table    
    users_table = df_log.selectExpr('userId as user_id', 'firstName as first_name', \
                                    'lastName as last_name', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'tables/users.parquet'))

    # create timestamp column from original timestamp column
    df_log = df_log.withColumn("timestamp", to_timestamp(df.ts / 1000))
    
    # extract columns to create time table
    time_table = df_log.selectExpr('timestamp as start_time', 'hour(timestamp) as hour', \
                               'dayofmonth(timestamp) as day', 'weekofyear(timestamp) as week', \
                               'month(timestamp) as month', 'year(timestamp) as year', \
                               'dayofweek(timestamp) as weekday').dropDuplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(os.path.join(output_data, 'tables/times.parquet'))

    # read in song data to use for songplays table
    #song_data = input_data + 'song_data/A/A/*/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json'
    df_songs = spark.read.json(song_data)
    
    # song and log data for songplays table
    sp_songs_table = df_songs.select('artist_name', 'artist_id', 'title', 'song_id').dropDuplicates()
    sp_log_table = df_log.selectExpr('artist', 'song', 'timestamp',  'userId', 'level',  'sessionId', \
                                 'location',  'userAgent', 'month(timestamp) as month', 'year(timestamp) as year').dropDuplicates()

    # Join the song and log dataframes, and transform
    songplays = sp_log_table.join(sp_songs_table, \
                                  (sp_log_table.artist == sp_songs_table.artist_name) & \
                                  (sp_log_table.song == sp_songs_table.title), \
                                  how = 'left').withColumn("songplay_id", monotonically_increasing_id())
 

    songplays_table = songplays.selectExpr('songplay_id', 'timestamp AS start_time', 'userId AS user_id', 'level', 'song_id', 'artist_id', \
                                       'sessionId AS session_id', 'location', 'userAgent AS user_agent', \
                                       'month', 'year').dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, 'tables/songplays.parquet'))
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://uda-out-apg/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
