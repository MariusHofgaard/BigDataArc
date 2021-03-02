"""

"""


from pyspark import SparkConf, SparkContext
import csv
import time

sc = SparkContext()
badges_table = sc.textFile("sourcefiles/badges.csv")
badges_table = badges_table.map(lambda convert_col: (convert_col[0], 1))

badges_table = badges_table.reduceByKey(lambda a,b: a+b)

less_than_three_badges = badges_table.filter(lambda x: x[1] < 3).count()

print(less_than_three_badges)