from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import concat, col, lit, desc,expr
from datetime import datetime

sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("spark_batch_data")
sparkConf.set("spark.driver.memory", "1g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")

# create the spark session, which is the entry point to Spark SQL engine.
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# Import the three different batch files
directors = spark.read.format("csv").option("header", "true").load("/home/jovyan/data/initial_directors.csv")
actors = spark.read.format("csv").option("header", "true").load("/home/jovyan/data/initial_actors.csv")
movies_genres = spark.read.format("csv").option("header", "true").load("/home/jovyan/data/initial_movies_genres.csv")
print(datetime.now(), "data loaded")

# Clean and change the directors data towards the right format for BigQuery
directors = directors.drop("_c0","index") #delete not used columns
directors = directors.withColumn("name", sf.concat(sf.col('first_name'),sf.lit(' '), sf.col('last_name'))) #merge the columns first_name and last_name in the new column name
directors = directors.drop("first_name","last_name") #these columns are not longer needed, since this data is now available in column name
directors = directors.where("gender = 'F'") #only female actors are of interest, so the male can be deleted
directors = directors.withColumn("id",directors.id.cast('int')) #change the type of column to int, since this in needed in the table
directors = directors.withColumn("year",directors.year.cast('int')) #change the type of column to int, since this in needed in the table
directors = directors.select("id", "name", "year","gender") #reorder the columns in logical order 
directors = directors.sort("id") #sort the data, such that it is imported sorted within the table in BigQuery

# Clean and change the actors data towards the right format for BigQuery
actors = actors.drop("index") #delete not used columns
actors = actors.withColumn("name", sf.concat(sf.col('first_name'),sf.lit(' '), sf.col('last_name'))) #merge the columns first_name and last_name in the new column name
actors = actors.drop("first_name","last_name") #these columns are not longer needed, since this data is now available in column name
actors = actors.where("gender = 'F'") #only female actors are of interest, so the male can be deleted
actors = actors.withColumn("id",actors.id.cast('int')) #change the type of column to int, since this in needed in the table
actors = actors.withColumn("year",actors.year.cast('int')) #change the type of column to int, since this in needed in the table
actors = actors.select("id", "name", "year","gender") #reorder the columns in logical order 
actors = actors.sort("id") #sort the data, such that it is imported sorted within the table in BigQuery

# Clean and change the movies_genres data towards the right format for BigQuery
movies_genres = movies_genres.drop("index") #delete not used columns
movies_genres = movies_genres.withColumn("id",movies_genres.id.cast('int')) #change the type of column to int, since this in needed in the table
movies_genres = movies_genres.withColumn("year",movies_genres.year.cast('int')) #change the type of column to int, since this in needed in the table
movies_genres = movies_genres.select("id", "genre", "year") #reorder the columns in logical order 
movies_genres = movies_genres.sort("id") #sort the data, such that it is imported sorted within the table in BigQuery

# Get the number of female actors each year
Female_actors = actors.where(actors.gender == "F").select("year","gender") #select only the female actors with only the columns year and gender since each row represent one female actor
Female_actors = Female_actors.groupby(Female_actors.year).count().orderBy(col("count").desc()) #get the count of female actors per year
Female_actors = Female_actors.selectExpr("year","count as Count_Female_actors")

# Get the number of female directors each year
Female_directors = directors.where(directors.gender == "F").select("year","gender")#select only the female directors with only the columns year and gender since each row represent one female director
Female_directors = Female_directors.groupby(Female_directors.year).count().orderBy(col("count").desc()) #get the count of female directors per year
Female_directors = Female_directors.selectExpr("year as year_d","count as Count_Female_directors")

#create one dataframe with the count of both female actors and directors per year
joinExpression = Female_actors["year"] == Female_directors['year_d'] 
Female_count = Female_directors.join(Female_actors, joinExpression,"inner").drop("year_d") #join the two seperate dataframes into one dataframe
Female_count = Female_count.select("year", "Count_Female_directors", "Count_Female_actors")
Female_count = Female_count.sort("year")

# print the status of the file 
print(datetime.now(), "data prepared")

bucket = "stream_tempga2"
spark.conf.set('temporaryGcsBucket', bucket)

# We need to set the following configuration whenever we need to use GCS.
# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Saving the directors data to BigQuery
# Delete records where the name is empty
directors.na.drop(subset='name') \
  .write.format('bigquery') \
  .option('table', 'group-4-325408.ga2.directors') \
  .mode("append") \
  .save()
print(datetime.now(), "Directors saved")

# Saving the actors data to BigQuery
# Delete records where the name is empty
actors.na.drop(subset='name') \
  .write.format('bigquery') \
  .option('table', 'group-4-325408.ga2.actors') \
  .mode("append") \
  .save()
print(datetime.now(), "actors saved")

# Saving the movies_genres data to BigQuery
# Delete records where the genre is empty
movies_genres.na.drop(subset='genre') \
  .write.format('bigquery') \
  .option('table', 'group-4-325408.ga2.movies_genres') \
  .mode("append") \
  .save()
print(datetime.now(), "movies_genres saved")

# Saving the female_count data to BigQuery
Female_count.write.format('bigquery') \
  .option('table', 'group-4-325408.ga2.female_count') \
  .mode("append") \
  .save()
print(datetime.now(), "female_count saved")
