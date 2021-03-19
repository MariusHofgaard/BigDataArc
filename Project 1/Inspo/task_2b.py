from pyspark import SparkConf, SparkContext
import base64


sc = SparkContext()

#b)
review_table = sc.textFile("sourcefiles/users.csv")
headers = review_table.first()
review_table = review_table.filter(lambda line: line != headers)

#removing first row (header)
review_table = review_table.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

# Average number of characters in a review
#splitting on column
review_texts = review_table.map(lambda line : (line.split("\t"))[3])

#mapping length of each string, and then reducing to count the total sum of characters
sum_chars = review_texts.map(lambda x : len(base64.b64decode(x))).reduce(lambda x, y : x + y)

#number of lines in table
num_lines = review_table.map(lambda line : line.split('\t')).count()

#average review length
avg_len=sum_chars/num_lines

print("Average review length: " + str(avg_len))

f = open('task_2b.csv', 'w')
with f:
	f.write(str(avg_len))