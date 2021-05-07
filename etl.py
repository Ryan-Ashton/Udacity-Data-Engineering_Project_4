import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates Spark Session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Retrieves song JSON data from S3 location and stores as a Parquet. Data is partitioned by year and artist id
    
    Arguments:
        spark: instantiates spark session
        input_data: Takes in path to S3 with contains Song data
        output_data: Exports to specified S3 bucket
        
    Returns:
        None
    
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(df.song_id, \
                            df.title, \
                            df.artist_id, \
                            df.year, \
                            df.duration).dropDuplicates(subset=["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet("{}songs".format(output_data))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude').dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("{}/artists".format(output_data))


def process_log_data(spark, input_data, output_data):
    """
        Description: Loads data, creates user, time and songplays table and stores as Parquet
    
    Arguments:
        spark: instantiates spark session
        input_data: Takes in path to S3 with contains log data
        output_data: Exports to specified S3 bucket
        
    Returns:
        None
    """
    # get filepath to log data file
    log_data = "{}log-data/*/*/*.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == "NextSong")

    # extract columns for users table    
    users_table = df.select('userId',
                            'firstName',
                            'lastName',
                            'gender',
                            'level').dropDuplicates(subset=["userId"])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet("{}users".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    
    
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    
    # extract columns to create time table
    time_table = (
        df
        .withColumn("hour", hour("timestamp"))
        .withColumn("day", dayofmonth("timestamp"))
        .withColumn("week", weekofyear("timestamp"))
        .withColumn("month", month("timestamp"))
        .withColumn("year", year("timestamp"))
        .withColumn("weekday", dayofweek("timestamp"))
        .select("timestamp", "hour", "day", "week", "month", "year", "weekday")
        .distinct()
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet("{}time".format(output_data))

    # read in song data to use for songplays table
    song_df = spark.read.parquet('data/output_data/songs.pq')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT l.songplay_id, l.ts as start_time, 
        l.userId as user_id, l.level, s.song_id, s.artist_id, 
        l.sessionId as session_id, l.location, l.userAgent as user_agent, t.year, t.month
        FROM logs l 
        JOIN songs s ON s.title=l.song
        JOIN time_table t ON l.ts=t.start_time
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet("{}songplays".format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-rmashton/rmashton/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
