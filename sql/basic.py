""" A simple example demonstrating basic Spark SQL features.
"""
import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *


# Get the path of parent directory i.e. LearnSpark
DIR_PATH = os.path.dirname(os.getcwd())


def basic_df_example(spark):
    file_path = os.path.join(DIR_PATH, 'resources/people.json')
    df = spark.read.json(file_path)
    # Displays the content of the DataFrame to stdout
    df.show()

    # Print the schema in a tree format
    df.printSchema()

    # Select only the "name" column
    df.select('name').show()

    # Select everybody, but increment the age by 1
    df.select(df['name'], df['age'] + 1).show()

    # Select people older than 21
    df.filter(df['age'] > 21).show()

    # Count people by age
    df.groupBy("age").count().show()

    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    sql_df = spark.sql("SELECT * FROM people")
    sql_df.show()

    # Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    # Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()

    # Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()


def schema_inference_example(spark):
    # $example on:schema_inferring$
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    file_path = os.path.join(DIR_PATH, 'resources/people.txt')
    lines = sc.textFile(file_path)
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teen_names = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teen_names:
        print(name)
    # Name: Justin
    # $example off:schema_inferring$


def programmatic_schema_example(spark):
    # $example on:programmatic_schema$
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    file_path = os.path.join(DIR_PATH, 'resources/people.txt')
    lines = sc.textFile(file_path)
    parts = lines.map(lambda l: l.split(","))
    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1].strip()))

    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM people")

    results.show()


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf=SparkConf()) \
        .getOrCreate()
    
    basic_df_example(spark)
    schema_inference_example(spark)
    programmatic_schema_example(spark)

    spark.stop()
