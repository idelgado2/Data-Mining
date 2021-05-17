import os
import sys
import re
import string
import json
from pyspark import SparkContext
from pyspark.rdd import *
from pyspark.sql import *
from datetime import datetime

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

jsonPath = sys.argv[1]
output_file = sys.argv[2]
stopwords_file = sys.argv[3]
yearArg = int(sys.argv[4])
topUsersArg = int(sys.argv[5])
topWordsArg = int(sys.argv[6])


def makeDate(x):
    dateObj = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    return dateObj.year


def readStopWords(path):
    stopwords_array = []
    with open(path) as f:
        for line in f:
            stopwords_array.append(line.strip())
        return stopwords_array


def cleanReview(review):
    clean = re.sub('[()[\],.!?:;]', '', review)
    cut = clean.lower().split(' ')
    return cut

#reviewsJSON = '/Users/isaacdelgado/Documents/HW/review.json'
#reviewTestJSON = '/Users/isaacdelgado/Documents/HW/review_example.json'

sc = SparkContext('local[*]', 'task1')

sqlContext = SQLContext(sc)
sqlc = sqlContext.read.json(jsonPath).rdd


#### A. Total number of reviews ########
total_num_reviews = sqlc.count()
#print("Total number of reviews: %s" % total_num_reviews)
########################################

#### B. Total number of reviews in a given year y ########
datesRDD = sqlc.map(lambda entry: (makeDate(entry['date']), 1)).reduceByKey(lambda a, b: a+b)
total_num_reviews_year = datesRDD.lookup(yearArg)
#print("Total number of reviews for year y -> %s" % total_num_reviews_year) #change 2017 with the given y as input here
########################################

#### C. Total number of distinct users who have written the reviews ########
#### D. Top m users who have the largest number of reviews and its count ########
uniqueUsers = sqlc.map(lambda user: (user['user_id'], 1)) \
    .reduceByKey(lambda a, b: a + b).sortBy(lambda item: item[1], False)
total_num_uniqueUsers = uniqueUsers.count()
#print("Total distinct users -> %s" % total_num_uniqueUsers)  # number of unique users
topUsers = uniqueUsers.take(topUsersArg)    #replace the 3 for input m
#print("Top users -> %s" % topUsers)
###############################################################################

#### E. Top n frequent words in the review text. ########
stopwords = readStopWords(stopwords_file)
frequentWords = sqlc.map(lambda x: x['text']).\
    flatMap(cleanReview).\
    map(lambda x: (x, 1)).\
    filter(lambda x: x[0] not in stopwords).\
    filter(lambda x: x[0] not in '').\
    reduceByKey(lambda a, b: a+b).\
    sortBy(lambda item: item[1], False).take(topWordsArg) #replace the 3 for input n
#print("Top n frequent words -> %s" % frequentWords)
###############################################################################

ansJSON = {
    'A': total_num_reviews,
    'B': total_num_reviews_year[0],
    'C': total_num_uniqueUsers,
    'D': topUsers,
    'E': frequentWords
}

with open(output_file, 'w') as outfile:
    json.dump(ansJSON, outfile)

