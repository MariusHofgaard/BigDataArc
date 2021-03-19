
from pyspark import SparkContext


sc = SparkContext()

comments_table = sc.textFile("sourcefiles/comments.csv")
number_of_lines_comments = comments_table.map(lambda line : line.split('\t')).count()

comments_table_entropy =