"""

File created for TDT 4305 Big Data Architecture
Written by: Marius Hofgaard and Petter Norsted

"""

"""
You should take the following steps to construct the term graph for the input document:
1. Turn all the characters to lower case
2. Remove all the punctuations (like '!' and '?') except 'DOT' characters
3. Remove all the symbols (like '$' and '>') and special characters (like 'TAB')
4. Tokenise the output of the previous step (the separator of tokens is the 'WHITESPACE' character); at this
stage should have a sequence of tokens
5. Remove the tokens that are smaller than three characters long from the sequence of the tokens
6. Remove all the 'DOT' characters from the start or the end of each token
7. Remove the stopwords from the sequence of tokens (The list of stopwords is available at the end of this
document.)

"""

# Input file: Posts.csv.gz

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import countDistinct
import re
import base64


from datetime import datetime
from graphframes import *

sc = SparkContext()
spark = SparkSession(sc)

# import the posts
posts_file = sc.textFile("./sourcefiles/posts.csv")
posts_rdd = posts_file.map(lambda line: line.split("\t"))

# Reomoving header from posts
# posts_rdd = posts_rdd.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

# TODO lower of all text
posts_lower = posts_rdd.map(lambda line: (line[0], base64.b64decode(line[5]).lower()))


# Clean text for punctuation and symbols except from "DOT"

def eliminate_unwanted_symbols(line):
    """
    Task 1
    Input: string, could be the entire document
    Method: Removes all characters that are not: word characters, space characters or "DOT" aka "."

    Output: Cleaned string containing only "DOT" characters, words and whitespaces.
    """

    pre_processed = re.sub('<[^<]+?>', '', str(line[1][3:])) # removes html tags from text such as <p>, additionally removes teh first b character.

    new_string = re.sub(r'[^\w\s]', '',pre_processed)  # Removes everything that is not \w : word \s : spaces & \. : "DOT". It seems to me that every sentence starts with bp - thus [3:]

    new_string = re.sub(r'xaxa', " ", new_string) # After looking at the output there is a lot of xaxa w/o meaning. Seems to be a bit dangerous - as there would be words with xa.

    return (line[0],new_string) #



# Perform this opperation to the posts dataset

clean_text = posts_lower.map(lambda line: eliminate_unwanted_symbols(line))
# clean_text = posts_lower.map(lambda line: (line[0], trial(line[1])))
print(clean_text.count())
# Tokenise the output of the previous step (the separator of tokens is the 'WHITESPACE' character); at this stage should have a sequence of tokens


def tokenize_line(line):
    """
    Task 2
    Input: text, as string - w/o
    return: List of tokens
    """
    stringpart = re.split(' ' , str(line[1]))

    return (line[0],stringpart) # Splits the line for \s : spaces +: split for every sequence of spaces.

tokenized = clean_text.map(lambda x: tokenize_line(x) )


# in order to verify the outpyt
for get_random_lines in tokenized.take(3):
    print(get_random_lines)

# Importing stopwords.
stopwords = sc.textFile("./sourcefiles/stopwords.txt")  # From the github site
encoded_stopwords = stopwords.map(lambda x: x.encode('ascii', 'ignore')).collect()


# Remove the tokens that are smaller than three characters long from the sequence of the tokens
def remove_short_words(line):
    """

    :param line: inp
    :return:
    """
    for index,item in enumerate(line[1]): # enumerate the list for popping
        if len(item) < 3:
            line[1].pop(index)

        elif item in encoded_stopwords: # Removed the word if its a stopword.
            line[1].pop(index)

        elif re.search("http", item): # Removes HTTPS and urls
            line[1].pop(index)

    return line

tokenized_length_over_3 = tokenized.map(lambda line: remove_short_words(line) )

print(tokenized_length_over_3.count()) # Should return the same amount of rows, as the removed elements are within the row.




# def generate_sliding_windows(terms, window_size):
#     n_windows = (len(terms) - window_size) + 1
#     windows = []
#     for i in range(n_windows):
#         windows.append(terms[i:(i + window_size)])
#     return windows
#
# def generate_graph_tuples(windows):
#     graph = []
#     for window in windows:
#         for i in range(len(window)):
#             for j in range(i + 1, len(window)):
#                 if i != j:
#                     graph.append((window[i], window[j]))  # Edge from i to j
#                     graph.append((window[j], window[i]))  # Edge from j to i
#
#     return graph
#
# windows = generate_sliding_windows(unique_tokens.collect(), 5)  # Generate list of windows
# graph = generate_graph_tuples(windows)  # Generate edge tuples
# graph_rdd = sc.parallelize(graph)  # Create rdd from graph (ie. edge tuples)
#
#
#
# node = spark.createDataFrame(tuple_unique_tokens, ['id'])  # Create DF of vertices (distinct terms)
# edge = spark.createDataFrame(graph_rdd, ['src', 'dst'])  # Create DF of edges (edge tuples rdd)
#
# g = GraphFrame(node, edge)  # Create graphframes graph of vertices and edges
#
# print("Pagerank started at {}".format(datetime.now().time()))
# pr = g.pageRank(resetProbability=0.15, tol=0.0001)
# print("Pagerank completed at {}".format(datetime.now().time()))
# print("Top 20 vertices with pagerank value below")
# pr.vertices.sort('pagerank', ascending=False).show()