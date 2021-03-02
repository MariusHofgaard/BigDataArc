from pyspark import SparkConf, SparkContext

sc = SparkContext()

business_table = sc.textFile("yelp_businesses.csv")


#removing first row (header)
business_table = business_table.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

#splitting on tabs
business_table = business_table.map(lambda line : line.split('\t'))

#mapping postal codes into tuples with longitudes and latituves
postalcode_tuples = business_table.map(lambda line : (line[5],(float(line[6]),float(line[7]))))

#reducing tuples to accumulate longitudes and latitudes
sum_lat_long_per_postalcode = postalcode_tuples.reduceByKey(lambda accum, x : (accum[0] + x[0], accum[1] + x[1]))

#number of times each postal code appears
postalcode_appearance = business_table.map(lambda line : (line[5],1)).reduceByKey(lambda x, accum : x + accum)

#average og longitudes and latitudes per postal code (Geograpical Centroid)
GC_postalcode = sum_lat_long_per_postalcode.join(postalcode_appearance).mapValues(lambda x : ((x[0][0] / x[1]), (x[0][1] / x[1]))).collect()

f = open('task_3c.csv', 'w')
with f:
	f.write("postal code" + ',' "longitudes" + ',' + "latitudes" + '\n') 
	for i in GC_postalcode:
		print(i)
		f.write(str(i[0]) + ',' + str(i[1][0]) + ',' + str(i[1][1]) + '\n') 