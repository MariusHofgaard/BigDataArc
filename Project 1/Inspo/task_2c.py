from pyspark import SparkConf, SparkContext

sc = SparkContext()


#c)
review_table = sc.textFile("yelp_top_reviewers_with_reviews.csv")

#removing first row (header)
review_table = review_table.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])


#The 10 businesses with the most number of reviews
#splitting on column
business_id = review_table.map(lambda line : (line.split("\t"))[2])

#Mapping each business into a tuple of its id and 1 (id, 1)
id_tuples = business_id.map(lambda id : (id,1))

#reducing the tuples to sum the counts
id_count = id_tuples.reduceByKey(lambda accum, n : accum + n)

#Sorting by value, descending
id_count_sorted = id_count.sortBy(lambda x : x[1], ascending=False)

f = open('task_2c.csv', 'w')
with f:
	for i in id_count_sorted.take(10):
		print i[0]
		f.write(i[0] + "\n")
