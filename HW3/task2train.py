import os
import sys
import time
import math
import re
import json
from pyspark import SparkContext
from pyspark.sql import *

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

input_file = '/Users/isaacdelgado/Documents/HW/HW3/data/train_review_demo.json'
# input_file = sys.argv[1]  # json file format - test_review.json
# output_file = sys.argv[2]  # model_output_file
output_file = '/Users/isaacdelgado/Documents/HW/HW3/task2_model_demo'
stopwords_file = '/Users/isaacdelgado/Documents/HW/HW3/data/stopwords'
# stopwords_file =  sys.argv[3]  # stopwords


def print_items_list(my_list, label):
    print("\n=== {0} ===".format(label))
    for item in my_list:
        print(item)
    print("=" * (len(label) + 8))


def print_items_line(my_list, label):
    print("\n{0} -> {1}\n".format(label, my_list))


def read_stopwords(path):
    stopwords_array = []
    with open(path) as f:
        for line in f:
            stopwords_array.append(line.strip())
        return stopwords_array


def combine_text(entry):
    full_text = ""
    for word in entry[1]:
        full_text += word
        full_text += " "
    return entry[0], full_text  # return a tuple of business and total review text


def clean_review(review, s_words):
    temp_list = []
    for word in review[1].split():  # split concatenated reviews into individual words
        temp_str = re.sub('[()[\],.\'/\-#%+$!*?&:;"]', '', word).lower()
        if temp_str not in s_words and not temp_str.isdigit() and temp_str != '':  # remove stopwords and numbers
            temp_list.append(temp_str)
    return review[0], temp_list


def get_each_tf(term_list):
    temp_dict = {}
    for term in term_list:
        try:
            temp_dict[term] += 1
        except:
            temp_dict[term] = 1
    sorted_dict = sorted(temp_dict.items(), key=lambda x: x[1], reverse=True)
    return sorted_dict


def get_tf(term_list):
    temp_list = []
    max_term = term_list[1][0][1]  # since terms are sorted, first one is max frequency value
    for term in term_list[1]:
        word = term[0]
        frequency = term[1]
        # temp_list.append((term_list[0], word, frequency/max_term))
        temp_list.append((word, (term_list[0], frequency / max_term)))
        # temp_list.append(((word, frequency / max_term), term_list[0]))
    return temp_list


def get_tf_idf(entry, total_docs):
    # entry example: (1, 'time', 2, 0.8), (0, 'time', 2, 0.6666666666666666)
    business_id = entry[1][1][0]
    term = entry[0]
    doc_frequency = entry[1][0]
    term_frequency = entry[1][1][1]
    if doc_frequency/total_docs <= 0.000001:
        return -1, -1
    idf = math.log2(total_docs / doc_frequency)
    tf_idf = term_frequency * idf
    # temp.append((business_id, term, doc_frequency, term_frequency))
    return business_id, (term, tf_idf)


def convert_to_dictionary(term_tuple_list):
    di = dict(term_tuple_list)
    return di


def get_top_200(entry):
    term_dict = convert_to_dictionary(entry[1])
    sorted_terms = sorted(term_dict, key=term_dict.__getitem__, reverse=True)
    if len(sorted_terms) <= 200:
        return entry[0], sorted_terms
    else:
        return entry[0], sorted_terms[:200]


def make_profile(bus, vocab):
    profile = []
    for term in bus[1]:
        profile.append(vocab[term])
    return bus[0], profile


def convert_to_index(bus_list, bus_dict):
    temp = []
    for b in bus_list:
        temp.append(bus_dict[b])
    return temp


def get_vectors(user_tuple, business_profile):
    temp_dict = dict(business_profile)
    temp_list = []
    for bus in user_tuple[1]:
        temp_list.append(temp_dict[bus])
    return user_tuple[0], temp_list, len(temp_list)


def combine_vectors(x):
    user_id = x[0]
    vectors = x[1]
    num_businesses = x[2]
    temp_dict = {}
    for v in vectors:
        for term in v:
            try:
                temp_dict[term] += 1
            except:
                temp_dict[term] = 1

    # Export user profiles here since it will not fit in memory
    for term, value in temp_dict.items():
        temp_dict[term] = value/num_businesses
    return user_id, temp_dict
    # d = {
    #     "type": 'user',
    #     "user_id": user_id,
    #     "user_profile": temp_dict
    # }
    # with open(output_file, 'a') as file:
    #     json.dump(d, file)
    #     file.write("\n")


sc = SparkContext('local[*]', 'task2train')
sqlc = SQLContext(sc)
start_time = time.time()
stopwords = read_stopwords(stopwords_file)
sqlc_rdd = sqlc.read.json(input_file).rdd

# Map out business_ids with indexes
business_mapped = sqlc_rdd.map(lambda entry: entry['business_id']) \
    .sortBy(lambda bus: bus) \
    .distinct() \
    .zipWithIndex() \
    .map(lambda x: {x[0]: x[1]}) \
    .flatMap(lambda x: x.items())

total_businesses = business_mapped.count()
business_list = business_mapped.collectAsMap()  # collectAsMap so we can call the index by the business id

# Clean and separate terms for all reviews for each business
business_list_terms = sqlc_rdd.map(lambda entry: (business_list[entry['business_id']], entry['text'])) \
    .groupByKey() \
    .mapValues(list) \
    .map(combine_text) \
    .map(lambda x: clean_review(x, stopwords)) \
    .map(lambda x: (x[0], get_each_tf(x[1]))) \
    .flatMap(get_tf)

# Calculate TF_IDF value for all terms and take the top 200 of them
business_list_tf_idf = business_list_terms.map(lambda x: (x[0], x[1][0])) \
    .groupByKey() \
    .mapValues(list) \
    .map(lambda x: (x[0], len(x[1]))) \
    .join(business_list_terms) \
    .map(lambda x: get_tf_idf(x, total_businesses)) \
    .filter(lambda x: x[0] >= 0)\
    .groupByKey() \
    .mapValues(list)\
    .map(lambda x: get_top_200(x))

# Get total vocabulary for indexing in prediction script - like parallel array
total_vocab = business_list_tf_idf.flatMap(lambda x: [term for term in x[1]])\
    .distinct()\
    .sortBy(lambda x: x)\
    .zipWithIndex()\
    .collect()

total_vocab_dict = convert_to_dictionary(list(total_vocab))

# Make business profiles,
# Profile is made from indexes of total vocabulary dictionary
# Boolean matrix will be build in prediction script
business_profiles = business_list_tf_idf\
    .map(lambda x: make_profile(x, total_vocab_dict))\
    .collect()

# Create user profiles, will be similar to business profiles
# Made of indexes from vocabulary dictionary
user_list = sqlc_rdd.map(lambda entry: (entry['user_id'], entry['business_id']))\
    .groupByKey()\
    .mapValues(list)\
    .mapValues(lambda x: convert_to_index(x, business_list))\
    .map(lambda x: get_vectors(x, business_profiles))\
    .map(combine_vectors).collect()





# Export data to json file #
business_list_raw = list(business_list.keys())
business_list_json = {
    "type": 'business_list',
    "business_list": business_list_raw
}
with open(output_file, 'w') as outfile:
    json.dump(business_list_json, outfile)
    #json.dump(vocab_json, outfile)
    outfile.write("\n")
for item in business_profiles:
    data = {
        "type": 'business',
        "business_id": item[0],
        "business_profile": item[1]
    }
    with open(output_file, 'a') as outfile:
        json.dump(data, outfile)
        outfile.write("\n")



print("Duration: %s seconds" % round(time.time() - start_time))
