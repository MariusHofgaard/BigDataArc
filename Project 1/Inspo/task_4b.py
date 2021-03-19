from pyspark import SparkConf, SparkContext
from numpy import median

sc = SparkContext()

friendship_graph = sc.textFile("yelp_top_users_friendship_graph.csv")


#removing first row (header)
friendship_graph = friendship_graph.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

#splitting on comma
friendship_graph = friendship_graph.map(lambda line : line.split(','))

#grouping by node so each node is in tuple with all its destination nodes
destination_tuples = friendship_graph.groupByKey().map(lambda x : (x[0], list(x[1])))

number_of_nodes = destination_tuples.keys().count()
destinations = destination_tuples.values()

#sum of all out degrees
total_number_of_outdegrees = destinations.map(lambda x : len(x)).reduce(lambda y,z : y+z)

mean_outdegrees = total_number_of_outdegrees/number_of_nodes
print(mean_outdegrees)

outdegrees_sorted_list = destination_tuples.map(lambda x : len(x[1])).sortBy(lambda x : x, ascending=False)

median_outdegrees = median(outdegrees_sorted_list.collect())
print(median_outdegrees)


#indegrees
sum_destinations = destinations.flatMap(lambda list: list)
indegrees = sum_destinations.countByValue().values()

mean_indegrees = sum(indegrees)/sum_destinations.distinct().count()
print(mean_indegrees)

#median
indegrees_sorted = sorted(indegrees)
median_indegrees = median(indegrees_sorted)
print(median_indegrees)

f = open('task_4b.csv', 'w')
with f:
	f.write("mean outdegrees" + ',' + str(mean_outdegrees) +'\n' +
			"median outdegrees" + ',' + str(median_outdegrees) +'\n' +
			"mean indegrees" + ',' + str(mean_indegrees) +'\n' +
			"median indegrees" + ',' + str(median_indegrees)
			) 