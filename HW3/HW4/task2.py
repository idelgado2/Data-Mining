import os
import sys
import time
from collections import deque
from itertools import combinations
from pyspark import SparkContext

# Girvan-Newman Algorithm
# - Visit each node X once
# - Compute the number of shortest paths from X to each of the other nodes that go through each of the edges
# Then remove high betweenness edges

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'


def make_graph(nodes_dict, size, connections):
    g = []
    for i in range(size):   # make empty adjacency graph
        g.append([0 for i in range(size)])
    for p in connections:   # add edge between users, bi-directional (symmetrical)
        user_1 = nodes_dict[p[0]]
        user_2 = nodes_dict[p[1]]
        g[user_1][user_2] = 1
        g[user_2][user_1] = 1
    return g


def get_bfs_values(vertex, g):
    queue = deque([vertex])
    visited = {vertex: 1}
    level = {vertex: 0}
    parent = {vertex: None}
    while queue:
        v = queue.popleft()
        for n in g[v]:
            if n not in visited:
                queue.append(n)
                visited[n] = 1
                level[n] = level[v] + 1
                parent[n] = [v]
            elif n not in parent[v] and level[v] != level[n]:
                parent[n].append(v)
                visited[n] += 1
    return visited


filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]  # ub_sample_data.csv
output_file_betweeness = sys.argv[3]
output_file_community = sys.argv[4]

sc = SparkContext('local[*]', 'task1')
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
            vertices.append(user_x_id)
            seen.append(user_x_id)
        if user_y_id not in seen:
            vertices.append(user_y_id)
            seen.append(user_y_id)
        edges.append((user_x_id, user_y_id))

dictionary_vertices = dict(zip(vertices, range(len(vertices))))
graph = make_graph(dictionary_vertices, len(vertices), edges)
converted_list = {vertices[i]: [vertices[j] for j in range(len(graph[i])) if graph[i][j]] for i in range(len(vertices))}

rdd = sc.parallelize(vertices).map(lambda x: get_bfs_values(x, converted_list))


print("Duration: %s seconds" % round(time.time() - start_time))

print("filter_threshold -> %s" % filter_threshold)
print("input_file -> %s" % input_file)
print("output_file_betweeness -> %s" % output_file_betweeness)
print("output_file_community -> %s" % output_file_community)
