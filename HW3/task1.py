import os
import sys
import time
from pyspark import SparkContext
from pyspark.sql import *

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

input_file = '/Users/isaacdelgado/Documents/HW/HW3/data/train_review_demo.json'
# input_file = sys.argv[1]    # json file format
# output_file = sys.argv[2]  # json file format
output_file = 'task1.res'  # json file format
num_of_hash_functions = 5


def print_items(my_list):
    for item in my_list:
        print(item)


def print_items_label(my_list, label):
    print("{0} -> {1}".format(label, my_list))


def hash_columns(subset, col):
    column_list = []
    temp_list = []
    hash_list = []
    similar_columns_list = []
    for sub in subset:
        for column in range(len(sub[1])):  # get all business column values into one long list
            x = sub[1][column]
            column_list.append(x)
    for k in range(col):  # for every column
        current_index = k
        while current_index <= (len(column_list) - 1):
            temp_list.append(column_list[current_index])  # aggregate column values to their own list
            current_index += col  # get next column value - e.g col = 2, index = 0, then next value is index = 2
        hash_list.append(sum(temp_list) * 3 % col)  # hash the column to a value bucket
        temp_list.clear()  # clear temp list for next row
    for value in set(hash_list):
        similar_columns = [p for p in range(len(hash_list)) if hash_list[p] == value]
        similar_columns_list.append(tuple(similar_columns))
    return similar_columns_list


def sim(subset, sets_sim):
    sim_list = []
    signature_list = [item for item in subset]  # collect my signature matrix to a list here
    for t in sets_sim:  # for all similar candidate pairs
        intersection = 0
        unique_values = []
        seen_sets = []
        if len(t) == 2:  # make sure were looking at a similar pair
            col_1 = t[0]  # columns (businesses) to compare
            col_2 = t[1]
            for sub in signature_list:  # for all signature rows (hash functions) in signature matrix
                value_1 = sub[1][col_1]     # get signature values for each column we are comparing
                value_2 = sub[1][col_2]
                if value_1 == value_2 and value_1 not in seen_sets:  # add to intersection value
                    intersection += 1
                    seen_sets.append(value_1)
                    if value_1 not in unique_values:  # check to see if we counted one of these values in our union
                        unique_values.append(value_1)
                elif value_1 not in unique_values:
                    unique_values.append(value_1)
                elif value_2 not in unique_values:
                    unique_values.append(value_2)
        if not not unique_values:
            sim_list.append((seen_sets, unique_values))
    return sim_list


def hash_band(band, column_num):
    yield hash_columns(band, column_num)


def get_sim(x, sets):
    yield sim(x, sets)


def f(iterator):
    for x in iterator:
        print(x)
    print("===")


sc = SparkContext('local[*]', 'task1')
sqlc = SQLContext(sc)
start_time = time.time()
sqlc_rdd = sqlc.read.json(input_file).rdd

# Collect Data from JSON Files
users = sqlc_rdd.map(lambda entry: (entry['user_id'])).distinct()
business = sqlc_rdd.map(lambda entry: (entry['business_id'])).distinct()
marker = sqlc_rdd.map(lambda entry: (entry['user_id'], entry['business_id'])).groupByKey(). \
    map(lambda x: (x[0], list(x[1]))).collect()

# Total Unique Users and Businesses
users_total = users.count()
business_total = business.count()


# Create List from Data
user_list = users.collect()
business_list = business.collect()
tuple_matrix = []
signature_matrix = []


# Create Empty Signature Matrix
for hash_row in range(num_of_hash_functions):
    signature_matrix.append((hash_row, [200] * business_total))

# Create Empty Boolean Matrix - (columns as business sets and rows as users reviews)
for user in user_list:
    tuple_matrix.append((user, [0] * business_total))

# Fill Boolean Matrix
for user_tuple in marker:
    user_index = user_list.index(user_tuple[0])
    for business_item in user_tuple[1]:
        business_index = business_list.index(business_item)
        tuple_matrix[user_index][1][business_index] += 1

# Min-Hash Boolean Matrix to Signature Matrix
for row in tuple_matrix:
    hash_value_list = []
    for i in range(num_of_hash_functions):  # calculate hash values using row number of user
        hash_value = ((i * i) * (tuple_matrix.index(row)) + 1) % users_total  # hash function ((ax + 1) % #of users)
        hash_value_list.append(hash_value)
    for column_index in range(len(row[1])):  # look at every business for this users
        if row[1][column_index] == 1:  # if user reviewed this business
            for index in range(len(hash_value_list)):  # check to see if we update signature matrix
                test = signature_matrix[index][1][column_index]
                if hash_value_list[index] < test:  # if new hash value is less that the previously stored then replace
                    signature_matrix[index][1][column_index] = hash_value_list[index]

# Divide Signatures Matrix into b Partitions where b= bands
bands = 20
signature_matrix_rdd = sc.parallelize(signature_matrix)
signature_matrix_partitioned = signature_matrix_rdd.partitionBy(bands, lambda x: ((x + 1) % bands))
similar_rdd = set(signature_matrix_partitioned.mapPartitions(lambda x: hash_band(x, business_total)).
                  flatMap(lambda x: x).collect())

# Collect the business IDs of the found similar pairs
similar_business = []
num_col = 0
for j in similar_rdd:  # for each tuple pair of similar businesses
    if len(j) == 2:  # make sure we only get pairs
        num_col += 1    # count the number of pairs
        similar_business.append((business_list[j[0]], business_list[j[1]]))

# Compute Intersection and Union Values using MapPartitions
signature_matrix_rdd_test = sc.parallelize(signature_matrix). \
    mapPartitions(lambda x: get_sim(x, similar_rdd)).collect()

# Reduce Intersection and Union Values
sim_value = []
inter = [[]] * num_col
unique = [[]] * num_col
for each in signature_matrix_rdd_test:  # distribute the intersection and unique values to two list
    index = 0
    for k in each:
        for r in k[0]:  # intersection
            if r not in inter[index]:
                inter[index].append(r)
        for r in k[1]:  # unique items
            if r not in unique[index]:
                unique[index].append(r)
        index += 1

# Aggregate All The Data Into One List of Tuples
similar_business_list = []
for s in range(len(similar_business)):
    sim = len(inter[s]) / len(unique[s])  # Compute Jaccard Similiarity
    similar_business_list.append((similar_business[s][0], similar_business[s][1], sim))

# Write Results To Output File
f = open(output_file, "w")
for s in similar_business_list:
    f.write("{{\'b{0}\': \'{1}\', \'b{2}\': \'{3}\', \'sim\': {4}}}\n".
            format(business_list.index(s[0]) + 1, s[0], business_list.index(s[1]) + 1, s[1], s[2]))

print("Duration: %s seconds" % round(time.time() - start_time))
