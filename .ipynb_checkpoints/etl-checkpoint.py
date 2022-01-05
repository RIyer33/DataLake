import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import dayofweek, monotonically_increasing_id, from_unixtime
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Return a SparkSession object."""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark

def inspect_df(title, df, n=5):
    """Print out first n rows of the Dataframe.
    Arguments:
    title -- Label applied to table
    df    -- Dataframe to display.
    Keyword Arguments:
    n -- number of rows to display"""

    print(f'{title.upper()}:\n{df.show(n)}')
    
def process_song_data(spark, input_data, output_data):
    """Process the song data storing song and artist dimension tables.
    
    Arguments:
    spark       -- SparkSession object
    input_data  -- path to the raw song data files
    output_data -- path to write out the dimesion tables"""
    
    # get filepath to song data file
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    #song_data = os.path.join(input_data,'song_data/A/A/A/*.json')
    
    song_schema = StructType([
        StructField("song_id",StringType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),        
        StructField("year", IntegerType())
    ])
    
    # read song data file
    df = spark.read.json(song_data,schema=song_schema)

    # extract columns to create songs table
    song_fields = ["song_id","title","artist_id","year","duration"]
    songs_table = df.select(song_fields).dropDuplicates()
    inspect_df('songs_table', songs_table)
    
    # write songs tabsongs_tablele to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artist_fields = ["artist_id",
                     "artist_name as name",
                     "artist_location as location",
                     "artist_latitude as latitude",
                     "artist_longitude as longitude"
                    ]
    artists_table = df.selectExpr(artist_fields).dropDuplicates()
    inspect_df('artists_table', artists_table)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """Process the event log data storing users, time and songplay dimension tables.
    
    Arguments:
    spark       -- SparkSession object
    input_data  -- path to the raw event log data files
    output_data -- path to write out the resulting dimesion tables and fact table"""

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name","gender","level"]
    users_table = log_df.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    log_df = log_df.withColumn("timestamp",get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    log_df = log_df.withColumn("start_time", get_datetime(log_df.ts))
    
    # extract columns to create time table
    log_df = log_df.withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time"))
    
    time_table = log_df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data + "time")

       
    #read in the parquet files for song and artist
    song_table_df = spark.read.parquet(os.path.join(output_data, "songs/"))
    inspect_df('song_table_df', song_table_df)
    artist_table_df = spark.read.parquet(os.path.join(output_data, "artists"))
    inspect_df('artist_table_df', artist_table_df)
    
    #join on song and artist and selecting all required columns
    song_artist_table_df = (song_table_df.join(artist_table_df,'artist_id').select(['song_id', 'title', 'duration', 'artist_id', 'name']))
    
        
    #extract columns from joined song and log datasets to create songplays table
    e_df = log_df.alias('e_df')
    # show e_df
    inspect_df('e_df', e_df)
        
    sa_df = song_artist_table_df.alias('sa_df')
    #show combined song artist table 
    inspect_df('sa_df', sa_df)
    
    #set condition
    cond = [e_df.song == sa_df.title, e_df.length == sa_df.duration]
    cols = [
        'start_time','userId', 'level', 
        'song_id', 'artist_id', 'sessionId', 
        'location', 'userAgent',
        'year','month']
    songplay_table_df = (e_df.join(sa_df, cond,"left")).select(cols)
    songplay_table_df = songplay_table_df.withColumn("song_play_id", monotonically_increasing_id())
    #Show songplay table
    inspect_df('songplay_table_df', songplay_table_df)
    
    # Write to parquet file
    songplay_table_df.write.mode("overwrite").partitionBy("year","month").parquet(output_data + "songplay")
    
    
def main():
    """Allows program to be run locally or remotely on AWS EMR cluster."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://ri-datalake-project-s3/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
