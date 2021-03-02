from pyspark import SparkContext, SparkConf
import base64

def task2_1():
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    folder_name        = "./sourcefiles/"
    posts_file_name    = "posts.csv"
    comments_file_name = "comments.csv"

    posts_file = sc.textFile(folder_name + posts_file_name)
    posts_rdd  = posts_file.map(lambda line: line.split("\t"))

    comment_header     = sc.textFile(folder_name + comments_file_name).first()
    comments_file      = sc.textFile(folder_name + comments_file_name)
    comments_no_header = comments_file.filter(lambda line: not str(line).startswith(comment_header))
    comments           = comments_no_header.map(lambda line: line.split("\t"))

    questions = posts_rdd.filter(lambda line: line[1] == "1")
    answers   = posts_rdd.filter(lambda line: line[1] == "2")

    decoded_questions = questions.map(lambda line: str(base64.b64decode(line[5]))).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_answers   = answers.map(lambda line: str(base64.b64decode(line[5]))).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_comments  = comments.map(lambda line: str(base64.b64decode(line[2])))

    length_questions = decoded_questions.map(lambda line: len(line))
    length_answers   = decoded_answers.map(lambda line: len(line))
    length_comments  = decoded_comments.map(lambda line: len(line))

    avg_length_questions = length_questions.reduce(lambda a, b: a + b) / length_questions.count()
    avg_length_answers   = length_answers.reduce(lambda a, b: a + b) / length_answers.count()
    avg_length_comments  = length_comments.reduce(lambda a, b: a + b) / length_comments.count()

    print("Average length questions: {} ".format(int(avg_length_questions)))
    print("Average length answers  : {} ".format(int(avg_length_answers)))
    print("Average length comments : {} \n".format(int(avg_length_comments)))

task2_1()