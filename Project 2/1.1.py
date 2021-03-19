"""

File created for TDT 4305 Big Data Architecture
Written by: Marius Hofgaard and Petter Norsted

"""

"""
You should take the following steps to construct the term graph for the input document:
1. Turn all the characters to lower case x
2. Remove all the punctuations (like '!' and '?') except 'DOT' characters  x
3. Remove all the symbols (like '$' and '>') and special characters (like 'TAB') x
4. Tokenise the output of the previous step (the separator of tokens is the 'WHITESPACE' character); at this 
stage should have a sequence of tokens x
5. Remove the tokens that are smaller than three characters long from the sequence of the tokens : M
6. Remove all the 'DOT' characters from the start or the end of each token : M 
7. Remove the stopwords from the sequence of tokens (The list of stopwords is available at the end of this
document.) : P 
8. Create a Graph 

9. 

"""

# Input file: Posts.csv.gz

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import countDistinct
import re

sc = SparkContext()
spark = SparkSession(sc)

# import the posts
posts_file = sc.textFile("/sourcefiles/posts.csv")
posts_rdd = posts_file.map(lambda line: line.split("\t"))

# Reomoving header from posts
# posts_rdd = posts_rdd.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

# TODO lower of all text
posts_lower = posts_rdd.map(lambda line: (x[0], base64.b64decode(x[5]).lower()))


# Clean text for punctuation and symbols except from "DOT"


def eliminate_unwanted_symbols(string):
    """
    Task 1
    Input: string, could be the entire document
    Method: Removes all characters that are not: word characters, space characters or "DOT" aka "."

    Output: Cleaned string containing only "DOT" characters, words and whitespaces.
    """

    new_string = re.sub(r'[^\w\s\.]', '', string)  # Removes everything that is not \w : word \s : spaces & \. : "DOT"
    return new_string



# Perform this opperation to the posts dataset

clean_text = posts_lower.map(lambda line: (line[0], eliminate_unwanted_symbols(line[1])))



# Tokenise the output of the previous step (the separator of tokens is the 'WHITESPACE' character); at this stage should have a sequence of tokens


def tokenize_line(line):
    """
    Task 2
    Input: text, as string - w/o
    return: List of tokens
    """

    return re.split(r'\s+' , line)  # Splits the line for \s : spaces +: split for every sequence of spaces.

tokenized = clean_text.map(lambda line: (line[0], tokenize_line(line[1])))

# Remove the tokens that are smaller than three characters long from the sequence of the tokens

# tokenized_length_over_3 = tokenized.map(lambda token: if len(token) < 3):

def remove_dot(string):
    """
    Task 1
    Input: string, could be the entire document
    Method: Removes all characters that are not: word characters, space characters or "DOT" aka "."

    Output: Cleaned string containing only "DOT" characters, words and whitespaces.
    """

    new_string = re.sub(r'[\w\s]', '', string)  # Removes everything that is not \w : word \s : spaces
    return new_string





# # reading the stop words file, encoding in ascii and representing as an array
#
# stopwords = sc.textFile("stopwords.txt")  # From the github site
# stopwords = stopwords.map(lambda x: x.encode('ascii', 'ignore')).collect()
#
# # mapping all reviews and filtering out stopwords
# posts = posts.map(lambda x: (x[0], [xi for xi in x[1].lower().split() if xi not in stopwords]))
# #
#





