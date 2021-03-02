from pyspark import SparkConf, SparkContext
#from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

business_df = spark.read.option("delimiter", "\t").csv("yelp_businesses.csv", inferSchema = True, header = True)

review_df = spark.read.option("delimiter", "\t").csv("yelp_top_reviewers_with_reviews.csv", inferSchema = True, header = True) 

friendship_df = spark.read.csv("yelp_top_users_friendship_graph.csv", inferSchema = True, header = True) 

business_df.show()
review_df.show()
friendship_df.show()