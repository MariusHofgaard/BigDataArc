"""

"""
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext


sc = SparkContext()

spark = SparkSession(sc)


# Retrieve the users.csv file

users_table = spark.read.option("delimiter", "\t").csv("sourcefiles/users.csv", inferSchema = True, header = True)
users_table.show()

# Remove the header

# Generate a overview

# Retrieve the comments.csv file
comments_table = sc.textFile("sourcefiles/comments.csv")

# Remove the header

# Generate a overview
comments_overveiw = comments_table.map(lambda line : line.split('\t'))

