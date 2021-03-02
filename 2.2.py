from pyspark import SparkContext, SparkConf
from datetime import datetime as dt

#Converts str to dateform: YYYY-mm-dd HH:MM:SS
def str_to_time(date_str):
    return dt.strptime(date_str, "%Y-%m-%d %H:%M:%S")

def task2_2():
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    folder_name = "./sourcefiles/"
    posts_file_name = "posts.csv"
    users_file_name = "users.csv"

    posts_file = sc.textFile(folder_name + posts_file_name)
    posts_rdd = posts_file.map(lambda line: line.split("\t"))
    users_file = sc.textFile(folder_name + users_file_name)
    users_rdd = users_file.map(lambda line: line.split("\t"))

    questions = posts_rdd.filter(lambda line: line[1] == "1")

    oldest_question = questions.reduce(
        lambda a, b: a if str_to_time(a[2]) < str_to_time(b[2]) else b)
    newest_question = questions.reduce(
        lambda a, b: a if str_to_time(a[2]) > str_to_time(b[2]) else b)

    oldest_question_user = users_rdd.filter(
        lambda user: user[0] == oldest_question[6]).collect()[0]
    newest_question_user = users_rdd.filter(
        lambda user: user[0] == newest_question[6]).collect()[0]

    print("Oldest question: " + oldest_question[2] + " posted by " +oldest_question_user[3])
    print("Newest question: " + newest_question[2] + " posted by " +newest_question_user[3])


task2_2()