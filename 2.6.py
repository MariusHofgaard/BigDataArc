"""

Calculate the entropy of the users w. at least one comment
"""

from pyspark import SparkContext
import math

sc = SparkContext()


def task2_6():
    comments_table = sc.textFile("sourcefiles/comments.csv")

    # Total length of comments

    total_length = comments_table.count()

    # Removing header
    comments_table = comments_table.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])


    # Splitting on UserId
    comments_table_split = comments_table.map(lambda line : (line.split("\t"))[4])

    # Mapping each user into a touple for counting
    user_tuples = comments_table_split.map(lambda user: (user,1))

    # Reduction for finding num* comments per user
    comments_per_user = user_tuples.reduceByKey(lambda user, accum : accum + user)


    # Calculate the entropy for the entire RDD.
    entropy = - (comments_per_user.map(lambda x: x[1] / total_length * math.log(x[1] / total_length, 2)).sum() )
    return entropy

print ("The entropy of the RDD is: ", round(task2_6(),4))

