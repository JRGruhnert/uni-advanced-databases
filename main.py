from rdflib import Graph
import rdflib
import time
from join import hash_join, sort_merge_join


file_path = "/home/jangruhnert/Documents/watdiv.10M/watdiv.10M.nt"
#file_path = "/home/jangruhnert/Documents/watdiv.10M/saved.txt"

def parse_rdf_data(file_path):
    g = Graph()
    g.parse(file_path, format="nt")
    triples = list(g)
    return triples

# Build a dictionary for string value transformation.
def build_string_dictionary(triples):
    string_dict = {}
    next_id = 0
    for triple in triples:
        subject, property, obj = triple
        # Check if subject and object are strings and not already in the dictionary
        if isinstance(subject, str) and subject not in string_dict:
            string_dict[subject] = next_id
            next_id += 1

        if isinstance(obj, str) and obj not in string_dict:
            string_dict[obj] = next_id
            next_id += 1

    return string_dict

# Step 3: Vertically partition the triples based on properties.
def vertically_partition(triples):
    property_tables = {}
    for triple in triples:
        subject, property, object = triple
        if property not in property_tables:
            property_tables[property] = []

        property_tables[property].append({'Subject': subject, 'Object': object})

    return property_tables
#
# Step 4: Combine all steps and preprocess the data.
def preprocess_data(file_path):
    triples = parse_rdf_data(file_path)
    print("Data parsed")
    string_dict = build_string_dictionary(triples)
    print("String_dict built")
    property_tables = vertically_partition(triples)
    print("Property tables built")
    return string_dict, property_tables


# Measure execution time for a function
def measure_execution_time(func, *args):
    start_time = time.time()
    result = func(*args)
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time

# Usage:
#result_hash_join = hash_join(property_tables['follows'], property_tables['friendOf'], join_column)
#result_sort_merge_join = sort_merge_join(property_tables['follows'], property_tables['friendOf'], join_column)

# Usage:
join_column = 'Object'  # Assuming 'Object' column is used for joining
follows = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/follows')
friendOf = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/friendOf')
string_dict, property_tables = preprocess_data(file_path)
#property_tables[follows] = property_tables[follows][0:1000000]
#property_tables[friendOf] = property_tables[friendOf][0:1000000]
print("Data preprocessed")

#result_hash_join, time_hash_join = measure_execution_time(hash_join, property_tables[follows], property_tables[friendOf], join_column)
#result_hash_join = None
#print("Time hash Join finished!")
#print("Time for time hash join: " + str(time_hash_join))

#result_sort_merge_join, time_sort_merge_join = measure_execution_time(sort_merge_join, False, property_tables[follows], property_tables[friendOf], string_dict, join_column)
#result_sort_merge_join = None
#print("Sort merge join finished!")
#print("Time for sort merge join: " + str(time_sort_merge_join))

result_sort_merge_parallel_join, time_sort_merge_parallel_join = measure_execution_time(sort_merge_join, True, property_tables[follows], property_tables[friendOf], string_dict, join_column)
result_sort_merge_parallel_join = None
print("Sort merge join parallel finished!")
print("Time for sort merge parallel join: " + str(time_sort_merge_parallel_join))