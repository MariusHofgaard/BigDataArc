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

#Mapping each date-time object into a tuple of its year and 1 (year, 1)
yearTuples = review_table_split_formated.map(lambda line : (line.year,1))

#Sorting by value, descending
reviewsPerYear = yearTuples.reduceByKey(lambda year, accum : accum + year).collect()


f = open('task_2d.csv', 'w')
with f:
	for i in reviewsPerYear:
		print i
		f.write(str(i[0]) + ',' + str(i[1]) + "\n")
