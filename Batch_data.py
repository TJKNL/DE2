{\rtf1\ansi\ansicpg1252\cocoartf2636
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fmodern\fcharset0 Courier-Bold;}
{\colortbl;\red255\green255\blue255;\red15\green112\blue1;\red242\green242\blue242;}
{\*\expandedcolortbl;;\cssrgb\c0\c50196\c0;\cssrgb\c96078\c96078\c96078;}
\paperw11900\paperh16840\margl1440\margr1440\vieww20420\viewh9080\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\b\fs26 \cf2 \cb3 \expnd0\expndtw0\kerning0
from pyspark.sql import SparkSession\
from pyspark import SparkConf\
from pyspark.sql import functions as sf\
from pyspark.sql.types import IntegerType\
from pyspark.sql.functions import concat, col, lit, desc,expr\
\
"""
This file was used to create the .csv files used in our demo.
"""

sparkConf = SparkConf()\
sparkConf.setMaster("spark://spark-master:7077")\
sparkConf.setAppName("spark_batch_data")\
sparkConf.set("spark.driver.memory", "1g")\
sparkConf.set("spark.executor.cores", "1")\
sparkConf.set("spark.driver.cores", "1")\
\
# create the spark session, which is the entry point to Spark SQL engine.\
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()\
\
# Import the three different batch files\
directors = spark.read.format("csv").option("header", "true").load("/home/jovyan/data/initial_directors.csv")\
actors = spark.read.format("csv").option("header", "true").load("/home/jovyan/data/initial_actors.csv")\
movies_genres = spark.read.format("csv").option("header", "true").load("/home/jovyan/data/initial_movies_genres.csv")\
print("data loaded")\
# Clean and change the directors data towards the right format\
\
directors = directors.drop("_c0","index")\
directors = directors.withColumn("name", sf.concat(sf.col('first_name'),sf.lit(' '), sf.col('last_name')))\
directors = directors.drop("first_name","last_name")\
directors = directors.where("gender = 'F'")\
directors = directors.withColumn("id",directors.id.cast('int'))\
directors = directors.withColumn("year",directors.year.cast('int'))\
directors = directors.select("id", "name", "year","gender")\
directors = directors.sort("id")\
\
# Clean and change the actors data towards the right format\
actors = actors.drop("index")\
actors = actors.withColumn("name", sf.concat(sf.col('first_name'),sf.lit(' '), sf.col('last_name')))\
actors = actors.drop("first_name","last_name")\
actors = actors.where("gender = 'F'")\
actors = actors.withColumn("id",actors.id.cast('int'))\
actors = actors.withColumn("year",actors.year.cast('int'))\
actors = actors.select("id", "name", "year","gender")\
actors = actors.sort("id")\
\
# Clean and change the movies_genres data towards the right format\
movies_genres = movies_genres.drop("index")\
movies_genres = movies_genres.withColumn("id",movies_genres.id.cast('int'))\
movies_genres = movies_genres.withColumn("year",movies_genres.year.cast('int'))\
movies_genres = movies_genres.select("id", "genre", "year")\
movies_genres = movies_genres.sort("id")\
\
# Get the number of female actors each year\
Female_actors = actors.where(actors.gender == "F").select("year","gender")\
Female_actors = Female_actors.groupby(Female_actors.year).count().orderBy(col("count").desc())\
Female_actors = Female_actors.selectExpr("year","count as Count_Female_actors")\
#Female_actors.show()\
\
# Get the number of female directors each year\
Female_directors = directors.where(directors.gender == "F").select("year","gender")\
Female_directors = Female_directors.groupby(Female_directors.year).count().orderBy(col("count").desc())\
Female_directors = Female_directors.selectExpr("year as year_d","count as Count_Female_directors")\
#Female_directors.show()\
\
#create one dataframe with both female actors and directors per year\
joinExpression = Female_actors["year"] == Female_directors['year_d']\
Female_count = Female_directors.join(Female_actors, joinExpression,"inner").drop("year_d")\
Female_count = Female_count.select("year", "Count_Female_directors", "Count_Female_actors")\
Female_count = Female_count.sort("year")\
\
print("data prepared")\
\
bucket = "stream_tempga2"\
spark.conf.set('temporaryGcsBucket', bucket)\
\
# Setup hadoop fs configuration for schema gs://\
conf = spark.sparkContext._jsc.hadoopConfiguration()\
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
\
# Saving the data to BigQuery\
# Delete records where the name is empty\
directors.write.format('bigquery') \\\
  .option('table', 'group-4-325408.ga2.directors') \\\
  .mode("append") \\\
  .save()\
print("Directors saved")\
\
# Delete records where the name is empty\
actors.na.drop(subset='name') \\\
  .write.format('bigquery') \\\
  .option('table', 'group-4-325408.ga2.actors') \\\
  .mode("append") \\\
  .save()\
print("actors saved")\
\
# Delete records where the name is empty\
movies_genres.na.drop(subset='genre') \\\
  .write.format('bigquery') \\\
  .option('table', 'group-4-325408.ga2.movies_genres') \\\
  .mode("append") \\\
  .save()\
print("movies_genres saved")\
\
Female_count.write.format('bigquery') \\\
  .option('table', 'group-4-325408.ga2.female_count') \\\
  .mode("append") \\\
  .save()\
print("female_count saved")\
}