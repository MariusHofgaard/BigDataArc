"""
File created for TDT 4305 Big Data Architecture
Written by: Marius Hofgaard and Petter Norsted
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime as dt

#Converts str to dateform: YYYY-mm-dd HH:MM:SS
def str_to_time(date_str):
    return dt.strptime(date_str, "%Y-%m-%d %H:%M:%S")

def task2_2():
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    # Import Posts
    posts_table = sc.textFile("sourcefiles/users.csv")
    posts_rdd = posts_table.map(lambda line: line.split("\t"))

    # Import users
    users_file = sc.textFile("sourcefiles/users.csv")
    users_rdd = users_file.map(lambda line: line.split("\t"))


    # Get questions from posts, evaluate for all where col 1 = 1
    questions = posts_rdd.filter(lambda line: line[1] == "1")


    # Find the oldest querstion by a sorting mechanism
    oldest_question = questions.reduce(lambda a, b: a if str_to_time(a[2]) < str_to_time(b[2]) else b)

    newest_question = questions.reduce(lambda a, b: a if str_to_time(a[2]) > str_to_time(b[2]) else b)

    oldest_question_user = users_rdd.filter(lambda user: user[0] == oldest_question[6]).collect()[0]

    newest_question_user = users_rdd.filter(lambda user: user[0] == newest_question[6]).collect()[0]


    print("Oldest question: " + oldest_question[2] + " posted by " + oldest_question_user[3])
    print("Newest question: " + newest_question[2] + " posted by " + newest_question_user[3])


task2_2()