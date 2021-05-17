import os
import time
import sys
from pyspark import SparkContext
from pyspark.sql import *
from graphframes import *

# Girvan-Newman Algorithm
# - Visit each node X once
# - Compute the number of shortest paths from X to each of the other nodes that go through each of the edges
# Then remove high betweenness edges

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11  pyspark-shell"


sc = SparkContext('local[*]', 'task1')
sql_context = SQLContext(sc)
start_time = time.time()

# Create a Vertex DataFrame with unique ID column "id"
v = sql_context.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
], ["id", "name", "age"])
# Create an Edge DataFrame with "src" and "dst" columns
e = sql_context.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
], ["src", "dst", "relationship"])

v_rdd = v.rdd.map(lambda entry: (entry[1], entry[0]))



# g = GraphFrame(v, e)
# result = g.labelPropagation(maxIter=5)
# result.sort(['label'], ascending=True).show()
# print("filter -> %s" % filter_threshold)
# print("input_file -> %s" % input_file)
# print("output_file -> %s" % output_file)

print("Duration: %s seconds" % round(time.time() - start_time))
