from pyspark import SparkContext, SparkConf
import base64


def task2_1():
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    posts_file = sc.textFile("/sourcefiles/posts.csv")

    # splitting on tabs
    posts_rdd = posts_file.map(lambda line: line.split("\t"))

    # removing header
    posts_rdd = posts_rdd.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

    comments_file = sc.textFile("/sourcefiles/comments.csv")
    # Splitting on tabs
    comments_file = comments_file.map(lambda line: line.split("\t"))

    # Removing header
    comments = comments_file.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

    questions = posts_rdd.filter(lambda line: line[1] == "1")
    answers = posts_rdd.filter(lambda line: line[1] == "2")

    decoded_questions = questions.map(lambda line: str(base64.b64decode(line[5]))).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_answers = answers.map(lambda line: str(base64.b64decode(line[5]))).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_comments = comments.map(lambda line: str(base64.b64decode(line[2])))

    length_questions = decoded_questions.map(lambda line: len(line))
    length_answers = decoded_answers.map(lambda line: len(line))
    length_comments = decoded_comments.map(lambda line: len(line))

    avg_length_questions = length_questions.reduce(lambda a, b: a + b) / length_questions.count()
    avg_length_answers = length_answers.reduce(lambda a, b: a + b) / length_answers.count()
    avg_length_comments = length_comments.reduce(lambda a, b: a + b) / length_comments.count()

    print(f"Average length questions: {int(avg_length_questions)} ")
    print(f"Average length answers  : {int(avg_length_answers)} ")
    print(f"Average length comments : {int(avg_length_comments)} ")


task2_1()