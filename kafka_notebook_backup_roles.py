from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, concat, col, lit
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from time import sleep

sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("spark_stream")
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")

# create the spark session, which is the entry point to Spark SQL engine.
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# We need to set the following configuration whenever we need to use GCS.
# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Read the whole dataset as a batch
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9093") \
        .option("subscribe", "test") \
        .option("startingOffsets", "earliest") \
        .load()

#content = lines.select(
#        (split(lines.value, ";").alias("index")))
split_col = split(df.value, ';')
dfr = df.withColumn('index', split_col.getItem(0).cast('int'))
dfr = dfr.withColumn('id', split_col.getItem(1).cast('int'))
dfr = dfr.withColumn('title', split_col.getItem(2))
dfr = dfr.withColumn('year', split_col.getItem(3).cast('int'))
dfr = dfr.withColumn('score', split_col.getItem(4).cast('float'))
dfr = dfr.withColumn('genre_id', split_col.getItem(5).cast('int'))
dfr = dfr.select('index', 'id', 'title', 'year', 'score', 'genre_id')


bucket = "stream_tempga2"
spark.conf.set('temporaryGcsBucket', bucket)

def my_foreach_batch_function(df, batch_id):
   # Saving the data to BigQuery as batch processing sink -see, use write(), save(), etc.
    df.na.drop(subset='score') \
      .write.format('bigquery') \
      .option('table', 'group-4-325408.streamtest.movies') \
      .mode("append") \
      .save()

# Write to a sink - here, the output is written to a Big Query Table
# Use your gcp bucket name.
# ProcessingTime trigger with two-seconds micro-batch interval
activityQuery = dfr.writeStream.outputMode("append") \
                    .trigger(processingTime = '2 seconds').foreachBatch(my_foreach_batch_function).start()
try:
    activityQuery.awaitTermination()
except KeyboardInterrupt:
    activityQuery.stop()
    # Stop the spark context
    spark.stop()
    print("Stoped the streaming query and the spark context")