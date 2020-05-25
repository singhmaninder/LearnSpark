"""
A simple example demonstrating Spark SQL data sources.
Run with:
  python datasource.py
"""
from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql import Row


# Get the path of parent directory i.e. LearnSpark
DIR_PATH = os.path.dirname(os.getcwd())


def basic_datasource_example(spark):
    file_path = os.path.join(DIR_PATH, 'resources/users.parquet')
    df = spark.read.load(file_path)
    df.select("name", "favorite_color").write.save("resources/namesAndFavColors.parquet")


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .config(conf=SparkConf()) \
        .getOrCreate()
    
    basic_datasource_example(spark)