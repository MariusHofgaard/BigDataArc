from pyspark import SparkConf, SparkContext
import csv
import time

sc = SparkContext()


time.sleep(10)
post_table = sc.textFile("sourcefiles/posts.csv")
number_of_lines_post = post_table.map(lambda line : line.split('\t')).count()

print(f'The number of lines in post.csv is {number_of_lines_post}')

comments_table = sc.textFile("sourcefiles/comments.csv")
number_of_lines_comments = comments_table.map(lambda line : line.split('\t')).count()

print(f'The number of lines in comments.csv is {number_of_lines_comments}')

badges_table = sc.textFile("sourcefiles/badges.csv")
number_of_lines_badges = badges_table.map(lambda line : line.split('\t')).count()

print(f'The number of lines in badges.csv is {number_of_lines_badges}')

users_table = sc.textFile("sourcefiles/users.csv")
number_of_lines_users = users_table.map(lambda line : line.split('\t')).count()

print(f'The number of lines in users.csv is {number_of_lines_users}')
