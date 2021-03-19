from pyspark import SparkConf, SparkContext

sc = SparkContext()

review_table = sc.textFile("yelp_top_reviewers_with_reviews.csv")


#d
#Number of reviews per year
review_table_split = review_table.map(lambda line : (line.split("\t"))[4])

from datetime import datetime

#removing first row (header)
review_table_split = review_table_split.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

#converting timestamp to date-time object
review_table_split_formated = review_table_split.map(lambda line : datetime.fromtimestamp(float(line)))

#Finding min and max date time
first_review = review_table_split_formated.min()
last_review = review_table_split_formated.max()
print(first_review)
print(last_review)

f = open('task_2e.csv', 'w')
with f:
	f.write(str(first_review) + "\n" + str(last_review))