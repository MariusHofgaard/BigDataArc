"""
File created for TDT 4305 Big Data Architecture
Written by: Marius Hofgaard and Petter Norsted
"""

from pyspark import SparkContext, SparkConf

def task2_3():
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    # import the posts
    posts_file = sc.textFile("/sourcefiles/posts.csv")
    posts_rdd  = posts_file.map(lambda line: line.split("\t"))

    # import the questions and answers for where col 1 == 1
    questions = posts_rdd.filter(lambda line: line[1] == "1")
    answers   = posts_rdd.filter(lambda line: line[1] == "2")

    # Group by UserID, then count number of posts before reduce to find the user
    gratest_number_of_answers = answers.groupBy(lambda line: line[6])\
        .map(lambda x: (x[0], len(list(x[1]))))\
        .sortBy(lambda x: x[1])\
        .reduce(lambda a, b: a if a[1] > b[1] else b)

    # Filter for NULL value in questions
    gratest_number_of_questions = questions.map(lambda a: (a[6], 1))\
        .filter(lambda x: x[0] != "NULL") \
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[1])\
        .reduce(lambda a, b: a if a[1] > b[1] else b)

    print("UserID most questions: " + str(gratest_number_of_questions[0]) +
          "\nNumber of questions: " + str(gratest_number_of_questions[1]))
    print("\nUserID most answers: " + str(gratest_number_of_answers[0]) +
          "\nNumber of questions: " + str(gratest_number_of_answers[1]))

task2_3()