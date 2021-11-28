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
# Read the whole dataset as a batch
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9093") \
        .option("subscribe", "test") \
        .option("startingOffsets", "earliest") \
        .load()

content = lines.select(
        (split(lines.value, ";").alias("index")))
split_col = split(df.value, ';')
dfr = df.withColumn('index', split_col.getItem(0))
dfr = dfr.withColumn('id', split_col.getItem(1))
dfr = dfr.withColumn('title', split_col.getItem(2))
dfr = dfr.withColumn('year', split_col.getItem(3))
dfr = dfr.select('index', 'id', 'title', 'year')


query = dfr.writeStream.format("console").start()
import time
time.sleep(60) # sleep 10 seconds
query.stop()
print('Done')