from pyspark import SparkConf, SparkContext

sc = SparkContext()


#a)
#Distinct number of users
review_table = sc.textFile("sourcefiles/users.csv")

#removing first row (header)
review_table = review_table.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

num_distinct_users = review_table.map(lambda line : (line.split("\t"))[1]).distinct().count()

print("Number of distinct users: " + str(num_distinct_users))

f = open('task_2a.csv', 'w')
with f:
	f.write(str(num_distinct_users))

