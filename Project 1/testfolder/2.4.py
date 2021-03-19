"""


4 Calculate the number of users who received less than three badges


I used dataframe

"""


from pyspark import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import countDistinct

sc = SparkContext()
spark = SparkSession(sc)


# Import the CSV as a dataframe. This could be solved with RDD and/or map, but wanted to check this method.
badges_df = spark.read.option("delimiter", "\t").csv("sourcefiles/badges.csv", inferSchema = True, header = True)

# Count the number of distinct badges for each user
gr = badges_df.groupBy("UserId").agg(countDistinct("Name"))


# Rename the column for better readibility
gr = gr.withColumnRenamed("count(Name)","numberOfBadges")

# Filter for all w. less than three badges
gr = gr.filter(gr.numberOfBadges < 3)


# counts the number of users. If included directly in the print there were some strange effects.
number_of_users_with_less_than_three_badges = gr.count()

# prints the number
print(number_of_users_with_less_than_three_badges)
