"""
File created for TDT 4305 Big Data Architecture
Written by: Marius Hofgaard and Petter Norsted
"""

from pyspark import SparkContext, SparkConf
import base64

def task2_1():
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    # Creating an RDD for the posts
    posts_table = sc.textFile("sourcefiles/posts.csv")
    posts_rdd  = posts_table.map(lambda line: line.split("\t"))

    # Creating RDD for comments
    comments_table = sc.textFile("sourcefiles/comments.csv")
    # Removing header row comments
    comments_table = comments_table.zipWithIndex().filter(lambda r: r[1] > 0).map(lambda r: r[0])

    # Splitting for tabs
    comments = comments_table.map(lambda line: line.split('\t'))


    # Get the questions and answers from the posts cile
    questions = posts_rdd.filter(lambda line: line[1] == "1")

    # Get the questions and answers from the posts rdd
    answers = posts_rdd.filter(lambda line: line[1] == "2")

    # decode from base64
    decoded_questions = questions.map(lambda line: str(base64.b64decode(line[5]))).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))

    # decode from base64
    decoded_answers   = answers.map(lambda line: str(base64.b64decode(line[5]))).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))

    # decode from base64
    decoded_comments  = comments.map(lambda line: str(base64.b64decode(line[2])))

    # Evaluate lengths of the different parameters and generate table w. values
    length_questions = decoded_questions.map(lambda line: len(line))
    length_answers   = decoded_answers.map(lambda line: len(line))
    length_comments  = decoded_comments.map(lambda line: len(line))


    # Reduce to sum for calculating the average
    avg_length_questions = length_questions.sum() / length_questions.count()
    avg_length_answers   = length_answers.sum() / length_answers.count()
    avg_length_comments  = length_comments.sum() / length_comments.count()

    # print the results
    print("Average length questions: " + str(avg_length_questions))
    print("Average length answers  : " + str(avg_length_answers))
    print("Average length comments : "+ str(avg_length_comments))

task2_1()