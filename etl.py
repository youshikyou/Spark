import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,monotonically_increasing_id
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def check(spark):
    """ check the parquet result
    
        Arguments:
            spark {object}: SparkSession object
        Returns:
            None
    """
    print("check the result")
    df = spark.read.parquet(DEST_S3_BUCKET + 'songplays/year=2018/month=11/*.parquet')
    df.head(5)

    
def process_song_data(spark, input_data, output_data):
    
    """ Process song data and create songs and artists table
    
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView("song_data_temp")
    
    # extract columns to create songs table song_id, title, artist_id, year, duration
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM song_data_temp   
        """)
    
    songs_table.printSchema()
    songs_table.show()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(path=output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM song_data_temp
        """)
    
    artists_table.printSchema()
    artists_table.show()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(path=output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    
    """ Process log data and create users, time and songplays table
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/2018/11/*.json')
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')
    
    df.createOrReplaceTempView("log_data_temp")
    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT userId AS user_id, firstName AS first_name,lastName AS last_name,gender,level
        FROM log_data_temp
    
    """)                  
    
    users_table.printSchema()
    users_table.show()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(path=output_data + "users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0),TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
                            
    # extract columns to create time table
    time_table = df.selectExpr("start_time",
                            "hour(start_time) as hour",
                            "dayofmonth(start_time) as day",
                            "weekofyear(start_time) as week",
                            "month(start_time) as month",
                            "year(start_time) as year",
                             "dayofweek(start_time) as weekday"
                           ).distinct()
    
    time_table.printSchema()
    time_table.show()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(path=output_data + "time_table.parquet")            
                            
    # read in song data to use for songplays table
    song_df = spark.sql(""" SELECT * FROM song_data_temp """)
                            
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), "inner")\
                        .distinct()\
                        .select('start_time', 'userId', 'level', 'song_id',\
                                'artist_id', 'sessionId','location','userAgent',\
                                year(df['start_time']).alias('year'), month(df['start_time']).alias('month'))\
                        .withColumn("songplay_id", monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(path=output_data + 'songplays.parquet')
    songplays_table.printSchema()
    songplays_table.show()
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkfooudacity/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
                            
    #check(spark)

if __name__ == "__main__":
    main()
