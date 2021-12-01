from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, concat, col, lit
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from time import sleep

# Define characteristics of the Spark job.
sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("spark_stream_roles")
sparkConf.set("spark.driver.memory", "1g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")

# create the spark session, which is the entry point to Spark SQL engine.
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# We need to set the following configuration whenever we need to use GCS.
# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Read the stream from the topic roles.
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9093") \
        .option("subscribe", "roles") \
        .option("startingOffsets", "earliest") \
        .load()

# Extract the data from the stream into a Spark stream df.
split_col = split(df.value, ';')
dfr = df.withColumn('id', split_col.getItem(0).cast('int'))
dfr = dfr.withColumn('actor_id', split_col.getItem(1).cast('int'))
dfr = dfr.withColumn('role', split_col.getItem(2))
dfr = dfr.select('id', 'actor_id', 'role')


bucket = "stream_tempga2"
spark.conf.set('temporaryGcsBucket', bucket)

def my_foreach_batch_function(df, batch_id):
    # Saving the data to BigQuery as batch processing.
    # Drop rows with na (NULL) in 'role'. If no role is known, we are not interested.
    df.na.drop(subset='role') \
      .write.format('bigquery') \
      .option('table', 'group-4-325408.ga2.roles') \
      .mode("append") \
      .save()

# Output is written to a Big Query Table.
# ProcessingTime trigger with 15-seconds micro-batch interval. Time based on logs from Spark.
activityQuery = dfr.writeStream.outputMode("append") \
                    .trigger(processingTime = '15 seconds').foreachBatch(my_foreach_batch_function).start()


try:
    activityQuery.awaitTermination()
except KeyboardInterrupt:
    activityQuery.stop()
    # Stop the spark context
    spark.stop()
    print("Stoped the streaming query and the spark context")