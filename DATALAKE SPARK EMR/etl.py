import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    It takes the song data from raw files (Json) and store it in a dataframe.
    From that dataframe it will create parquet files in S3 for songs and artists tables
    
    spark : SparkContext
    Input_data(str) : input data location 
    Output_data(str): input data location
    """
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data,mode='permissive')
    # permissive mode has been added to check if there any corrupt data
    print("debug print : Total rows imported ", df.count())

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.format('parquet').mode('overwrite').save(path=output_data+'songs',partitionBy=['year','artist_id'])
    print("debug print : songs parquet files created successfully")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    # columns have been renamed as per the "Project Description"
    artists_table=artists_table.withColumnRenamed('artist_name','name')\
                               .withColumnRenamed('artist_location','location')\
                               .withColumnRenamed('artist_latituce','latitude')\
                               .withColumnRenamed('artist_longitude','longitude') 
    
    # write artists table to parquet files
    artists_table.write.format('parquet').mode('overwrite').save(path=output_data+'artists')
    print("debug print : artists parquet files created successfully")


def process_log_data(spark, input_data, output_data):
    
    """
    It takes the log data from raw files (Json) and store it in a dataframe.
    From that dataframe it will create parquet files in S3 for users, time and songsplay tables
    
    spark : SparkContext
    Input_data(str) : input data location 
    Output_data(str): input data location
    """
    # get filepath to log data file
    log_data= input_data+'log_data/*/*/*.json'

    # read log data file
    # permissive mode has been added to check if there is any corrupt data
    df = spark.read.json(log_data,mode='permissive')
    print("debug print : Total rows imported ", df.count())
    
    # filter by actions for song plays
    df = df.filter(col('page')=='NextSong')

    # extract columns for users table    
    # columns have been renamed as per the "Project Description"
    users_table = df.select('userid', 'firstname', 'lastname', 'gender', 'level').distinct()
    users_table.withColumnRenamed('userid','user_id')\
               .withColumnRenamed('firstname','first_name')\
               .withColumnRenamed('lastname','last_name')
    
    # write users table to parquet files
    users_table.write.format('parquet').mode('overwrite').save(output_data+'users')
    print("debug print : users parquet files created successfully")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000), TimestampType())
    df = df.withColumn('start_time',get_timestamp('ts')) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000), DateType())
    df = df.withColumn('date',get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select('start_time',
                           hour("date").alias("hour"), 
                           dayofmonth("date").alias("day"), 
                           weekofyear("date").alias("week"), 
                           month("date").alias("month"),
                           year("date").alias("year"),
                           dayofweek("date").alias("weekday")
                        ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.format('parquet').mode('overwrite').save(path=output_data+'time',paritionBy=['year','month'])
    print("debug print : time parquet files created successfully")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    # permissive mode has been added to check if there is any corrupt data
    song_df = spark.read.json(song_data, mode='permissive')
    
    # temp tables have been created to to use spark sql to join these two tables easily
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('logs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  spark.sql(""" 
                                SELECT monotonically_increasing_id() as songplay_id,
                                l.start_time,
                                year(l.start_time) as year,
                                month(l.start_time) as month,
                                l.userid as user_id,
                                l.level as level,
                                s.song_id as song_id,
                                s.artist_id as artist_id,
                                l.sessionId as session_id,
                                l.location as location,
                                l.userAgent as user_agent
                                FROM logs l
                                JOIN songs s
                                ON (l.song = s.title
                                AND l.length = s.duration
                                AND l.artist = s.artist_name)
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.format('parquet').mode('overwrite').save(output_data + "songplays", partitionBy=["year", "month"])
    print("debug print : songplays parquet files created successfully")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
