from pyspark import SparkConf, SparkContext

sc = SparkContext()

friendship_graph = sc.textFile("yelp_top_users_friendship_graph.csv")


#removing first row (header)
friendship_graph = friendship_graph.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

#splitting on comma
friendship_graph = friendship_graph.map(lambda line : line.split(','))




#grouping by node so each node is in tuple with all its destination nodes
destination_tuples = friendship_graph.groupByKey().map(lambda x : (x[0], list(x[1])))

#largest outdegree
outegrees_sorted = destination_tuples.sortBy(lambda x : len(x[1]), ascending=False)


#in-degrees
destinations = destination_tuples.values()
#sum_destinations = destinations.reduce(lambda x, accum : x + accum)

sum_destinations = destinations.flatMap(lambda list: list)

indegrees = sum_destinations.countByValue()

indegrees_sorted = sorted(indegrees.items(), key=lambda x : x[1], reverse=True)


f = open('task_4a.csv', 'w')
with f:
	f.write("node" + ',' "outdegrees" +'\n') 
	for i in outegrees_sorted.take(10):
		print(i[0], len(i[1]))
		f.write(str(i[0]) + ',' + str(len(i[1])) + '\n')
	f.write("node" + ',' "indegrees" +'\n') 
	for i in indegrees_sorted[:10]:
		print(i[0], i[1])
		f.write(str(i[0]) + ',' + str(i[1]) + '\n')