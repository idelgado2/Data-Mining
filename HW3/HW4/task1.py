import os
import time
import sys
from pyspark import SparkContext
from itertools import combinations
from pyspark.sql import *
from graphframes import *

# Girvan-Newman Algorithm
# - Visit each node X once
# - Compute the number of shortest paths from X to each of the other nodes that go through each of the edges
# Then remove high betweenness edges

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11  pyspark-shell"


def get_pairs(combination_list, threshold):
    # pair = ((user_1, [bus_1, ...]), (user_2, [bus_1, ...]))
    vertices = []
    edges = []
    for pair in combination_list:
        user_x_id = pair[0][0]
        user_y_id = pair[1][0]
        user_x = set(pair[0][1])
        user_y = set(pair[1][1])
        if len(set(user_x) & set(user_y)) >= threshold:
            vertices.append(user_x_id)
            vertices.append(user_y_id)
            edges.append((user_x_id, user_y_id))
            edges.append((user_y_id, user_x_id))
    return vertices, edges


def sort_lists(x):
    user_id = x[0]
    user_list = list(x[1])
    user_list.sort()
    return user_id, user_list


filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]  # ub_sample_data.csv
output_file = sys.argv[3]

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('WARN')
start_time = time.time()
csv_rdd = sc.textFile(input_file).map(lambda entry: entry.split(','))
header_row = csv_rdd.first()
csv_rdd = csv_rdd.filter(lambda entry: entry != header_row) \
    .groupByKey() \
    .mapValues(list).collect()

combinations_csv = list(combinations(csv_rdd, 2))
vertices = []
edges = []
seen = []
for pair in combinations_csv:
    user_x_id = pair[0][0]
    user_y_id = pair[1][0]
    user_x = set(pair[0][1])
    user_y = set(pair[1][1])
    if len(set(user_x) & set(user_y)) >= filter_threshold:
        if user_x_id not in seen:
            vertices.append((user_x_id,))
            seen.append(user_x_id)
        if user_y_id not in seen:
            vertices.append((user_y_id,))
            seen.append(user_y_id)
        edges.append((user_x_id, user_y_id))
        edges.append((user_y_id, user_x_id))

# vertices.append(["id"])
# edges.append(["src", "dst"])

sql_context = SQLContext(sc)
v = sql_context.createDataFrame(vertices).toDF("id")
e = sql_context.createDataFrame(edges).toDF("src", "dst")

g = GraphFrame(v, e)
result = g.labelPropagation(maxIter=5)
result.sort(['label'], ascending=True)
result_rdd = result.rdd.map(lambda entry: (entry[1], entry[0])) \
    .groupByKey().map(lambda entry: sort_lists(entry)).sortBy(lambda x: (len(x[1]), x[1])).collect()
f = open(output_file, "w")
for i in result_rdd:
    f.write(','.join(map(repr, i[1])))
    f.write('\n')
f.close()

print("Duration: %s seconds" % round(time.time() - start_time))
