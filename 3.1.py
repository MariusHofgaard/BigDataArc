"""

"""
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

def task3_1():
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)

    folder_name = "./sourcefiles/"
    posts_file_name = "posts.csv"
    users_file_name = "users.csv"
    comments_file_name = "comments.csv"

    posts_file = sc.textFile(folder_name + posts_file_name)
    posts_rdd = posts_file.map(lambda line: line.split("\t"))
    users_file = sc.textFile(folder_name + users_file_name)
    users_rdd = users_file.map(lambda line: line.split("\t"))
    comments_file = sc.textFile(folder_name + comments_file_name)
    comments_rdd = comments_file.map(lambda line: line.split("\t"))


    users = users_rdd.filter(lambda line: not line[0] == '"Id"')
    comments = comments_rdd.filter(lambda comment: comment[0] != '"PostId"')\
                           .map(lambda comment: (comment[0], comment[4]))

    posts_with_comments = posts_rdd.filter(lambda post: post[11] != "0").map(
        lambda post: (post[0], post[6]))

    # (PostId, (OwnerUserId, CommentUserId))
    posts_and_comment = comments_rdd.join(posts_with_comments)

    # (CommentUserId, OwnerUserId)
    commentid_ownerid = posts_and_comment.map(lambda post: (post[1][0], post[1][1]))

    # Add weight (count comments from i to j) :     (CommentUserId, (OwnerUserId, Weight))
    temp_graph = commentid_ownerid.map(lambda row: (
        row, 1)).reduceByKey(lambda a, b: a + b)

