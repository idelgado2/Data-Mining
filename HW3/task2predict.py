import os
import sys
import time
import json
from pyspark import SparkContext
from pyspark.sql import *

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'


def attach_business(entry, b_list):
    # b_list is in format - [['H8umkLwMkM0vP5B8oPiozQ', 'zK7sltLeRRioqYwgLiWUIA']]
    business = b_list[0][int(entry['business_id'])]
    return business, entry['business_profile']


# input_file = '/Users/isaacdelgado/Documents/HW/HW3/data/test_review_demo.json'
input_file = sys.argv[1]  # json file format - test_review.json
# train_file = '/Users/isaacdelgado/Documents/HW/HW3/task2_model_demo'
train_file = sys.argv[2]  # trained file - task2_model
output_file = sys.argv[3]  # model_output_file = task2predict

sc = SparkContext('local[*]', 'task2predict')
sqlc = SQLContext(sc)
start_time = time.time()
model_json = sc.textFile(train_file)
sqlc_rdd = sqlc.read.json(input_file).rdd   # testing data - test_review,json


testing_data = sqlc_rdd.map(lambda entry: (entry['user_id'], entry['business_id']))
business_list = model_json.map(lambda x: json.loads(x))

user_data = model_json.map(lambda x: json.loads(x)).filter(lambda x: x['type'] == 'user')\
    .map(lambda entry: (entry['user_id'], entry['user_profile']))

business_data = model_json.map(lambda x: json.loads(x)).filter(lambda x: x['type'] == 'business')\
    .map(lambda x: attach_business(x, business_list))

# joined_rdd = testing_data.join(user_data)
joined_rdd = user_data.join(testing_data)

# #
# # for m in business_list.collect():
# #     print(m)
# print("========")
# for m in user_data.take(5):
#     print(m)
# print("========")
# for m in business_data.take(5):
#     print(m)
# print("========")
# for m in testing_data.take(5):
#     print(m)
# print("========")
# for m in joined_rdd.take(5):
#     print(m)


print("Duration: %s seconds" % round(time.time() - start_time))
