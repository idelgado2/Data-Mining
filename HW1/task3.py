import os
import sys
import json
from pyspark import SparkContext
from pyspark.sql import *

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

reviewJSON = sys.argv[1]
output_file = sys.argv[2]
paritioningScheme = sys.argv[3]
partitionNum = int(sys.argv[4])
mostReviewed = int(sys.argv[5])


sc = SparkContext('local[*]', 'task3')
sqlContext = SQLContext(sc)
sqlcRev = sqlContext.read.json(reviewJSON).rdd

if paritioningScheme == 'default':
    reviewRDD = sqlcRev.map(lambda x: (x["business_id"], 1)).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] > mostReviewed)
    partitionLen = reviewRDD.glom().map(len).collect()
elif paritioningScheme == 'customized':
    reviewRDD = sqlcRev.map(lambda x: (x["business_id"], 1)).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] > mostReviewed).partitionBy(partitionNum)
    partitionLen = reviewRDD.glom().map(len).collect()
    print(reviewRDD.getNumPartitions())
    print(partitionLen)

ansJSON = {
    'n_partitions': reviewRDD.getNumPartitions(),
    'n_items': partitionLen,
    'result': reviewRDD.collect()
}

with open(output_file, 'w') as outfile:
    json.dump(ansJSON, outfile)

