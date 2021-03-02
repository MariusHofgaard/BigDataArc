from pyspark import SparkConf, SparkContext
import csv

sc = SparkContext()

business_table = sc.textFile("badges.csv")

#removing first row (header)
business_table = business_table.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

#splitting on tabs
business_table = business_table.map(lambda line : line.split('\t'))

#mapping cities and their reviews into tuple pairs
city_reviews_tuples = business_table.map(lambda line : (line[3],float(line[8])))

#reducing tuples to accumulate reviews for each city ((city, num_reviews), sum_reviews)
cum_reviews_per_city = city_reviews_tuples.reduceByKey(lambda city, accum : city + accum)

#number of businesses per city
num_businesses_per_city = business_table.map(lambda line : (line[3],1)).reduceByKey(lambda city, accum : city + accum)

#average rating per city
avg_rating_per_city = cum_reviews_per_city.join(num_businesses_per_city).mapValues(lambda x : x[0] / x[1]).collect()

f = open('task_3a.csv', 'w')
with f:
	#writer=csv.writer(f)
	for i in avg_rating_per_city:
		print(i)
		f.write(str(i[0].encode('utf8')) + ',' + str(i[1]) + '\n')