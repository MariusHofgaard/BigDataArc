from pyspark import SparkConf, SparkContext

sc = SparkContext()

business_table = sc.textFile("sourcefiles/users.csv")

#removing first row (header)
business_table = business_table.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

#splitting on tabs
business_table = business_table.map(lambda line : (line.split('\t'))[10])

#mapping categories into tuples with 1
category_tuples = business_table.map(lambda category : (category,1))

# reducing tuples to accumulate categories
sum_in_each_category = category_tuples.reduceByKey(lambda category, accum : category + accum).sortBy(lambda x : x[1], ascending = False)

f = open('task_3b.csv', 'w')
with f:
	for i in sum_in_each_category.take(10):
		print(i)
		f.write(str(i[0]) + ',' + str(i[1]) + '\n')