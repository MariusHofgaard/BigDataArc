"""
File created for TDT 4305 Big Data Architecture
Written by: Marius Hofgaard and Petter Norsted
"""

from pyspark import SparkContext, SparkConf

def task2_5():
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    # Import user file
    users_file = sc.textFile("sourcefiles/users.csv")

    # Split for \t
    users_rdd = users_file.map(lambda line: line.split("\t"))

    # Reomoving header from users
    users_rdd = users_rdd.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])


    # Get cols for just the upvotes and Just the downvotes.
    up_votes = users_rdd.map(lambda x: x[7])
    down_votes = users_rdd.map(lambda x: x[8])


    # Calculate the average
    average_up_votes = up_votes.reduce(lambda x, y: int(x) + int(y)) / up_votes.count()

    # Calculate the average
    average_down_votes = down_votes.reduce(lambda x, y: int(x) + int(y)) / down_votes.count()


    sum_top = users_rdd.map(lambda upvote: (int(upvote[7]) - average_up_votes)* (int(upvote[8]) - average_down_votes))\
                                          .reduce(lambda a, b: a + b)

    std_up_votes = users_rdd.map(lambda x: (int(x[7]) - average_up_votes) ** 2).reduce(lambda a, b: a + b) ** 0.5
    std_down_votes = users_rdd.map(lambda x: (int(x[8]) - average_down_votes) ** 2).reduce(lambda a, b: a + b) ** 0.5

    sum_bottum = std_up_votes * std_down_votes
    pearsons_r = sum_top / sum_bottum

    print("Person's r is: " + str(float(pearsons_r)))

task2_5()