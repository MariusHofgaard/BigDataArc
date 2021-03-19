from pyspark import SparkConf, SparkContext
import csv
import time

time.sleep(10)


sc = SparkContext()

business_table = sc.textFile("sourcefiles/badges.csv")
numLines_business = business_table.map(lambda line : line.split('\t')).count()

review_table = sc.textFile("sourcefiles/comments.csv")
numLines_review = review_table.map(lambda line : line.split('\t')).count()

friendship_graph = sc.textFile("sourcefiles/posts.csv")
numLines_graph = friendship_graph.map(lambda line : line.split(',')).count()

# friendship_graph = sc.textFile("sourcefiles/users.csv")
# numLines_graph = friendship_graph.map(lambda line : line.split(',')).count()

print("The 'yelp_businesses.csv' file has " + str(numLines_business) + " lines.")
print("The 'yelp_top_reviewers_with_reviews.csv' file has " + str(numLines_review) + " lines.")
print("The 'yelp_top_users_friendship_graph.csv' file has " + str(numLines_graph) + " lines.")

f = open('task_1a.csv', 'w')
with f:
    writer = csv.writer(f)
    writer.writerow([numLines_business, numLines_review, numLines_graph])