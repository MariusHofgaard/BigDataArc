"""

File created for TDT 4305 Big Data Architecture
Written by: Marius Hofgaard and Petter Norsted

"""




from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import re
import base64


# For enabling the requested running  method.
import sys

post_id_output = 14
try:
    for ind,arg in enumerate(sys.argv):
        if arg == "--post_id":
            post_id_output = sys.argv[i+1]
            print(post_id_output)
except:
    pass
sc = SparkContext()
spark = SparkSession(sc)



# import the posts
posts_file = sc.textFile("./sourcefiles/posts.csv")
posts_rdd = posts_file.map(lambda line: line.split("\t"))

# Reomoving header from posts
# posts_rdd = posts_rdd.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

posts_lower = posts_rdd.map(lambda line: (line[0], base64.b64decode(line[5]).lower()))


# Clean text for punctuation and symbols except from "DOT"

def eliminate_unwanted_symbols(line):
    """
    Task 1
    Input: string, could be the entire document
    Method: Removes all characters that are not: word characters, space characters or "DOT" aka "."

    Output: Cleaned string containing only "DOT" characters, words and whitespaces.
    """
    pre_processed = re.sub('<[^<]+?>', '', str(line[1][4:])) # removes html tags from text such as <p>, additionally removes teh first b character.

    new_string = re.sub(r'[^\w\s]', '',pre_processed)  # Removes everything that is not \w : word \s : spaces & \. : "DOT". It seems to me that every sentence starts with bp - thus [3:]

    new_string = re.sub(r'xaxa', " ", new_string) # After looking at the output there is a lot of xaxa w/o meaning. Seems to be a bit dangerous - as there would be words with xa.

    return (line[0],new_string)



# Perform this opperation to the posts dataset

clean_text = posts_lower.map(lambda line: eliminate_unwanted_symbols(line))



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


# Importing stopwords.
stopwords = sc.textFile("./sourcefiles/stopwords.txt")  # From the github site. Can't see the reasoning in  hardcoding.


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

        elif item in encoded_stopwords: # Removed the word if it is a stopword.
            line[1].pop(index)

        elif re.search("http", item): # Removes HTTPS and urls
            line[1].pop(index)

    return line

tokenized_length_over_3 = tokenized.map(lambda line: remove_short_words(line) ) # this line is particular slow due to the non-optimal for loop


output_post = tokenized_length_over_3.filter(lambda line: str(line[0]) == str(post_id_output))
print(output_post.take(1))


# print(tokenized_length_over_3.count()) # Should return the same amount of rows, as the removed elements are within the row.


### We did not manage to get the graphframe to function.

