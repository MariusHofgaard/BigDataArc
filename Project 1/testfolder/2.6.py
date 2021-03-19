"""

I assume the "total number of rows in the UserId column refers to the total number of rows in the initial comments
"""
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import countDistinct
from pyspark.sql.types import IntegerType
from pyspark.sql import udf
from org.apache.spark.sql.functions.udf import *

sc = SparkContext()
spark = SparkSession(sc)



def evaluate_entropy(x):
    """

    :param x: number of comments for spesific user
    :param tot: number of comments for all users
    :return: entropy

    """
    tot = 100

    return x / tot


# convert the UDF to a usable function for the df
# convertUDF = udf(lambda z: evaluate_entropy(z),IntegerType())


# Import the CSV as a dataframe
comments_df = spark.read.option("delimiter", "\t").csv("sourcefiles/comments.csv", inferSchema = True, header = True)

# for calculating total number
total_comments = comments_df.count()

# Count the number of distinct user comments. Select "creation date" as that is a unique value for each comment (per user).
df_counted_comments = badges_df.groupBy("UserId").agg(countDistinct("CreationDate"))

# Rename the column for better readibility
df_counted_comments = df_counted_comments.withColumnRenamed("count(Name)","numberOfComments")

# Filter for all w. less than three badges TODO check if nessesary
df_counted_comments = df_counted_comments.filter(df_counted_comments.numberOfBadges >= 1)


# Perform UDF
# df_counted_comments = df_counted_comments.withColumn("Entropy", convertUDF(col("numberOfComments"))).show(truncate=False)

rdd = df_counted_comments.rdd.map(toIntEmployee)
df_counted_comments.show()