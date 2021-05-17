import os
import sys
import csv
import json
from pyspark import SparkContext
from pyspark.rdd import *
from pyspark.sql import *

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'


def print_items(my_list):
    for item in my_list:
        print(item)


def print_items_label(my_list, label):
    print(label + " -> %s" % my_list)


def write_csv(line):    # make sure the file is empty before running this function as it will append
    with open(output_file, mode='a+') as my_file:
        my_writer = csv.writer(my_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        my_writer.writerow(line)


review_json_path = sys.argv[1]
business_json_path = sys.argv[2]
output_file = sys.argv[3]

sc = SparkContext('local[*]', 'task2')
sql_context = SQLContext(sc)
sql_rev = sql_context.read.json(review_json_path).rdd
sql_bus = sql_context.read.json(business_json_path).rdd

nevada_business = sql_bus.filter(lambda x: x['state'] == 'NV').map(lambda x: (x['business_id'], 1)) \
    .reduceByKey(lambda a, b: a + b)
reviews = sql_rev.map(lambda x: (x['business_id'], x['user_id']))

with open(output_file, mode='w+') as review_file:   # create output file and add headers to csv
    review_writer = csv.writer(review_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    review_writer.writerow(['user_id', 'business_id'])

joined_rdd = nevada_business.join(reviews).map(lambda x: (x[1][1], x[0])).map(write_csv).collect()

# print("review_json_path -> %s" % review_json_path)
# print("business_json_path -> %s" % business_json_path)
# print("output_file -> %s" % output_file)
