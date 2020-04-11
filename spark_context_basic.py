# Initializing a SparkContext
# Use spark-submit to run your application
# bin/spark-submit ~/Projects/LearnSpark/spark_context_basic.py

# Use the Python interpreter to run your application
# python spark_context_basic.py

import os
from pyspark import SparkConf, SparkContext

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

conf = SparkConf().setMaster("local").setAppName("spark-basics")

sc = SparkContext(conf=conf)

file_path = os.path.join(DIR_PATH, 'resources/content.md')
lines = sc.textFile(file_path)

# Calling the filter() transformation
pythonLines = lines.filter(lambda line: "Python" in line)

# Calling the first() action
pythonLines.persist
print(pythonLines.first())

print(pythonLines.count())

sc.stop()
