from pyspark import SparkConf, SparkContext
import math
import base64

sc = SparkContext()

review_table = sc.textFile("yelp_top_reviewers_with_reviews.csv")


#removing first row (header)
review_table = review_table.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])


#task_2e
#PCC formula
	#r_xy = (sum(xi - X)(yi - Y)) / (sqrt(sum(xi - X)**2) * sqrt(sum(yi - Y)**2))
	#     =  Covarianve(x,y) / std(x)*std(y)
	#where:
		#x is the number of reviews per user
		#y is the average number of the characters in the user's reviews
	#so:
		#xi : number of reviews for user i
		#X : avg length of all reviews
		#yi : average number characters in reviews of user i
		#Y : avg number of characters in all reviews


#NUMBER OF REVIEWS PER USER

#splitting on user id
review_table_split = review_table.map(lambda line : (line.split("\t"))[1])

#Mapping each user into a tuple of its (user, 1)
user_tuples = review_table_split.map(lambda user: (user,1))

#Reviews per user by reducing
reviews_per_user = user_tuples.reduceByKey(lambda user, accum : accum + user)
x = reviews_per_user



#AVG NUMBER OF REVIEWS
#number of users
num_users = reviews_per_user.count()

avg_number_of_reviews = reviews_per_user.map(lambda user : user[1]).sum() / num_users
X = float(avg_number_of_reviews)


#AVG REVIEW LENGHT PER USER
user_reviews = review_table.map(lambda line : (line.split("\t")))

#tuples with users and the length of their review 
user_review_lenght_tuple = user_reviews.map(lambda line: (line[1],len(base64.b64decode(line[3]))))

#total characters posted pr user
sum_characters_user_posted = user_review_lenght_tuple.reduceByKey(lambda user, accum : user + accum)

#avg review length pr user
avg_review_lenght_per_user = sum_characters_user_posted.join(reviews_per_user).mapValues(lambda x: x[0] / x[1])
y = avg_review_lenght_per_user


#AVERAGE REVIEW LENGTH (done in task_2b)
review_texts = review_table.map(lambda line : (line.split("\t"))[3])

#mapping length of each string, and then reducing to count the total sum of characters
cum_length_all_reviews = review_texts.map(lambda x : len(base64.b64decode(x))).reduce(lambda x, y : x + y)

#number of lines in table
num_lines = review_table.map(lambda line : line.split('\t')).count()

#average review length
Y = float(cum_length_all_reviews/num_lines)


#Covariance and Standard Deviations times sample (size-1)
cov_xy = x.join(y).mapValues(lambda x : (x[0] - X) * (x[1] - Y)).map(lambda x : x[1]).sum()
std_x = math.sqrt((x.map(lambda x : (x[1]-X)**2)).sum())
std_y = math.sqrt((y.map(lambda y : (y[1]-Y)**2)).sum())

#PCC
r_xy = cov_xy / (std_x*std_y)
print(r_xy)

f = open('task_2f.csv', 'w')
with f:
	f.write(str(r_xy))