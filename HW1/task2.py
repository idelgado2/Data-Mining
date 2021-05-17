import os
import re
import json
import sys
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql import DataFrame
from pyspark.rdd import RDD

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

reviewJSON = sys.argv[1]
businessJSON = sys.argv[2]
output_file = sys.argv[3]
sparkSwitch = sys.argv[4]
topCategories = int(sys.argv[5])

if sparkSwitch == 'spark':
    sc = SparkContext('local[*]', 'task2')
    sqlContext = SQLContext(sc)
    sqlcBus = sqlContext.read.json(businessJSON).rdd
    sqlcRev = sqlContext.read.json(reviewJSON).rdd


    def cleanReview(info):
        info[1] = re.sub(' ', '', info[1])
        info[1] = info[1].split(',')
        return info


    def cleanBusInfo(info):
        if (info['categories'] != None):
            result = [x.strip() for x in info['categories'].split(',')]
            return (info['business_id'], result)


    def addValues(tuple1, tuple2):
        sumTuple = tuple(map(lambda i, j: i + j, tuple1, tuple2))
        return sumTuple


    def calculateAverages(item):
        average = float(item[1][0]) / float(item[1][1])
        return (average, item[0])


    starRDD = sqlcRev.map(lambda x: (x["business_id"], x["stars"]))
    categoryRDD = sqlcBus.filter(lambda x: x['categories'] not in [None]).map(cleanBusInfo). \
        flatMap(lambda x: map(lambda y: (x[0], y), x[1]))
    joinedRDD = starRDD.join(categoryRDD)
    bigRDD = joinedRDD.map(lambda x: (x[1][1], (x[1][0], 1))).reduceByKey(addValues)
    finalRDD = bigRDD.map(calculateAverages).sortBy(lambda x: (-x[0], x[1])). \
        map(lambda y: (y[1], y[0])).take(topCategories)

    topRatedCategories = []
    for each_item in finalRDD:
        topRatedCategories.append(each_item)

    print("Top Rated Categories -> %s" % topRatedCategories)

    ansJSON = {
        'result': topRatedCategories
    }
    with open(output_file, 'w') as outfile:
        json.dump(ansJSON, outfile)

# elif sparkSwitch == 'no_spark':
#     revContent, busContent, combinedContent, resultList, \
#     allCategories, uniqueCategories, pastItems, finalList = [], [], [], [], [], [], [], []
#
#     def howMany(list, item):
#         return list.count(item)
#
#     with open(reviewJSON) as f:
#         for line in f:
#             revItem = json.loads(line)
#             revContent.append((revItem['business_id'], revItem['stars']))
#
#     with open(businessJSON) as f:
#         for line in f:
#             busItem = json.loads(line)
#             busContent.append((busItem['business_id'], busItem['categories']))
#     f.close()
#
#     for review in revContent:
#         for business in busContent:
#             if review[0] == business[0] and business[1] is not None:
#                 categoryList = [x.strip() for x in business[1].split(',')]
#                 for category in categoryList:
#                     # if category not in uniqueCategories:
#                     #     uniqueCategories.append(category)
#                     allCategories.append(category)
#                     combinedContent.append((category, review[1]))
#     for group in combinedContent:
#         count = 0
#         for categoryItem in combinedContent:
#             if group[0] in pastItems:
#                 continue
#             elif group[0] == categoryItem[0]:
#                 pastItems.append(group[0])
#                 count += float(group[1])
#         finalList.append(((count / howMany(allCategories, group[0])), group[0]))
#         count = 0
#     mylist = sorted(finalList, key=lambda x: (-x[0], x[1]))
#     for item in range(topCategories):
#         resultList.append((mylist[item][1], mylist[item][0]))
#     print(resultList)
#     ansJSON = {
#         'result': resultList
#     }
#     with open(output_file, 'w') as outfile:
#         json.dump(ansJSON, outfile)
else:
    print("** Wrong command line argument, type spark or no_spark instead **")
