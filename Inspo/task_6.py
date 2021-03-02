from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

business_table = spark.read.option("delimiter", "\t").csv("yelp_businesses.csv", inferSchema = True, header = True)

review_table = spark.read.option("delimiter", "\t").csv("yelp_top_reviewers_with_reviews.csv", inferSchema = True, header = True) 

friendship_graph = spark.read.csv("yelp_top_users_friendship_graph.csv", inferSchema = True, header = True) 


#task_6a & b
review_and_business_table = business_table.join(review_table, on=['business_id'], how='inner')
review_and_business_table.createOrReplaceTempView("review_and_business_table")

#task_6c
reviews_per_user = review_table.groupby('user_id').count().sort('count', ascending=False)
reviews_per_user.show()

reviews_per_user.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("task_6.csv")