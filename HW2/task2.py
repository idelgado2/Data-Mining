import os
import sys
import time
import itertools
from pyspark import SparkContext

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'


def true_frequent_dub(candidate_itemsets, data_set):
    data_set_list = list(data_set)  # full data to count itemsets - (user, [business])
    candidate_itemsets_list = list(candidate_itemsets)  # previously counted itemsets from first phase - (business, 1)
    # print_items_label(data_set_list, "data_sample_list")
    # print_items_label(candidate_itemsets_list, "candidate_itemsets")
    pair_items = {}
    for user in data_set_list:
        for item_set in candidate_itemsets_list:
            if type(item_set[0]) is str:
                if item_set[0] in user[1]:  # count singleton
                    if item_set[0] in pair_items:
                        pair_items[item_set[0]] += 1
                    else:
                        pair_items[item_set[0]] = 1
            else:  # else it is a tuple
                if set(item_set[0]).issubset(set(user[1])):  # count itemset
                    if item_set[0] in pair_items:
                        pair_items[item_set[0]] += 1
                    else:
                        pair_items[item_set[0]] = 1
    my_list = []
    for key in pair_items.keys():
        my_list.append((key, pair_items[key]))
    # print_items_label(mylist, "pair_items_tuple")
    return my_list


def trim_set(itemset, support):
    delete = [key for key in itemset if itemset[key] < support]  # remove itemsets that do not meet threshold
    for key in delete:
        del itemset[key]
    return itemset


def get_candidate_singletons(data_list, support):
    itemset = {}
    for user in data_list:
        for business in user[1]:
            if business in itemset:
                itemset[business] += 1
            else:
                itemset[business] = 1
    lean_item_set = trim_set(itemset, support)  # remove items that do not meet the support threshold
    return lean_item_set


def a_priori_dub(data_sample, global_support, total_count):
    data_sample_list = list(data_sample)
    support_sample = round((len(data_sample_list) / total_count) * global_support)  # p*s
    single_itemsets = get_candidate_singletons(data_sample_list, support_sample)
    if len(single_itemsets) != 0:
        flag = True
        k_count = 2
        while flag and k_count < 4:  # k_count >= 9 will cause long run time, also no need to go this far combinations
            pair_items = {}
            candidate_pairs = itertools.combinations(single_itemsets, k_count)
            for user in data_sample_list:
                for pair in candidate_pairs:
                    if set(pair).issubset(set(user[1])):  # use set() function to account for ordering in tuples
                        # print("pair: {} is a subset of basket items: {}".format(pair, user[1]))
                        if pair in pair_items:
                            pair_items[pair] += 1
                        else:
                            pair_items[pair] = 1
            if len(pair_items) != 0:
                candidate_sets = trim_set(pair_items, support_sample)  # remove itemsets that do not meet threshold
                single_itemsets.update(candidate_sets)
                k_count += 1  # set next item set length
                candidate_sets.clear()  # clear for next round
                pair_items.clear()  # clear for next round
                # print_items_label(candidate_sets, "candidate_sets")
            else:
                flag = False  # no more items to go through
        # print_items_label(candidate_sets, "pair_items")
    # print_items_label(single_itemsets, "single_items")
    return single_itemsets


def print_items(my_list):
    for item in my_list:
        print(item)


def print_items_label(my_list, label):
    print(label + " -> %s" % my_list)


filter_threshold = int(sys.argv[1])
support_threshold = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]

sc = SparkContext('local[*]', 'task1')
start_time = time.time()
csv_rdd = sc.textFile(input_file).map(lambda x: x.split(','))   # read user_business csv file to RDD
header_row = csv_rdd.first()

# Build basket_market model and filter results with only qualifying users - filter_threshold
csv_rdd = csv_rdd.filter(lambda x: x != header_row).map(lambda x: (x[0], x[1])).groupByKey().\
    map(lambda x: (x[0], list(x[1]))).filter(lambda x: len(x[1]) >= filter_threshold).sortByKey()
csv_rdd_count = csv_rdd.count()

# apply SON algorithm

map_reduce_1 = csv_rdd.mapPartitions(lambda x: a_priori_dub(x, support_threshold, csv_rdd_count)).\
    map(lambda x: (x, 1)).reduceByKey(lambda x, y: x)

full_data = csv_rdd.collect()  # make data into a list to pass to second phase map_reduce functions

map_reduce_2 = map_reduce_1.mapPartitions(lambda x: true_frequent_dub(x, full_data)).reduceByKey(lambda a, b: a + b) \
    .filter(lambda x: x[1] >= support_threshold).collect()

candidate_itemsets = map_reduce_1.collect()

candidates = [[], [], [], [], [], [], [], [], [], []]
for item in candidate_itemsets:
    if type(item[0]) is str:
        candidates[0].append(item[0])
    else:
        candidates[(len(item[0]) - 1)].append(item[0])

frequent_itemsets = [[], [], [], [], [], [], [], [], [], []]
for item in map_reduce_2:
    if type(item[0]) is str:
        frequent_itemsets[0].append(item[0])
    else:
        frequent_itemsets[(len(item[0]) - 1)].append(item[0])

# for ordered_list in candidates:
#     if len(ordered_list) != 0:
#         if type(ordered_list[0]) is int:
#             ordered_list.sort()
#         else:
#             ordered_list.sort(key=lambda t: t[0])
#
# for ordered_list in frequent_itemsets:
#     if len(ordered_list) != 0:
#         if type(ordered_list[0]) is int:
#             ordered_list.sort()
#         else:
#             ordered_list.sort(key=lambda t: t[0])
# write to output file

f = open(output_file, "w")
f.write("Candidates:\n")
for itemset in candidates:
    if len(itemset) != 0:
        set_length = len(itemset)
        count = 0
        for item in itemset:
            if type(item) is str:  # is singleton
                if count == (set_length - 1):
                    f.write("('" + item + "')")
                else:
                    f.write("('" + item + "'),")
                count += 1
            else:
                if count == (set_length - 1):
                    f.write("(")
                    for tuple_item in range(len(item)):
                        if tuple_item == (len(item) - 1):
                            f.write("'" + item[tuple_item] + "'")
                        else:
                            f.write("'" + item[tuple_item] + "',")
                    f.write(")")
                else:
                    f.write("(")
                    for tuple_item in range(len(item)):
                        if tuple_item == (len(item) - 1):
                            f.write("'" + item[tuple_item] + "'")
                        else:
                            f.write("'" + item[tuple_item] + "',")
                    f.write("),")
                count += 1
        f.write("\n\n")


f.write("Frequent Itemsets:\n")
for itemset in frequent_itemsets:
    if len(itemset) != 0:
        set_length = len(itemset)
        count = 0
        for item in itemset:
            if type(item) is str:  # is singleton
                if count == (set_length - 1):
                    f.write("('" + item + "')")
                else:
                    f.write("('" + item + "'),")
                count += 1
            else:
                if count == (set_length - 1):
                    f.write("(")
                    for tuple_item in range(len(item)):
                        if tuple_item == (len(item) - 1):
                            f.write("'" + item[tuple_item] + "'")
                        else:
                            f.write("'" + item[tuple_item] + "',")
                    f.write(")")
                else:
                    f.write("(")
                    for tuple_item in range(len(item)):
                        if tuple_item == (len(item) - 1):
                            f.write("'" + item[tuple_item] + "'")
                        else:
                            f.write("'" + item[tuple_item] + "',")
                    f.write("),")
                count += 1
        f.write("\n\n")
f.close()
print("Duration: %s seconds" % round(time.time() - start_time))

# print("filter_threshold -> %s" % filter_threshold)
# print("support_threshold -> %s" % support_threshold)
# print("input_file -> %s" % input_file)
# print("output_file -> %s" % output_file)
