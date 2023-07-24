import rdflib
import time
from data import preprocess_data
from join import hash_join, sort_merge_join

FILE_PATH = "/home/jangruhnert/Documents/watdiv.10M/watdiv.10M.nt"
JOIN_COLUMN1 = 'Object'  # Assuming 'Object' column is used for joining
JOIN_COLUMN2 = 'Subject'
FOLLOWS = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/follows')
FRIEND_OF = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/friendOf')
LIKES = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/likes')
HAS_REVIEW = rdflib.term.URIRef('http://purl.org/stuff/rev#hasReview')

# Measure execution time for a function
# Wraps the function and returns the result and the execution time
def measure_execution_time(func, *args):
    start_time = time.time()
    result = func(*args)
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time

def request_with_hash_join(property_tables, join_column1, join_column2):
    start_time = time.time()
    join = hash_join(property_tables[FOLLOWS], property_tables[FRIEND_OF], JOIN_COLUMN1, JOIN_COLUMN2)
    middle_time = time.time()
    join = hash_join(join, property_tables[LIKES], JOIN_COLUMN1, JOIN_COLUMN2)
    late_time = time.time()
    return hash_join(join, property_tables[HAS_REVIEW], JOIN_COLUMN1, JOIN_COLUMN2)

def request_with_sort_merge_join(parallel, property_tables, join_column1, join_column2):
    start_time = time.time()
    join = sort_merge_join(parallel, property_tables[FOLLOWS], property_tables[FRIEND_OF], JOIN_COLUMN1, JOIN_COLUMN2)
    middle_time = time.time()
    print("Join 1 finished")
    print("Time for join 1: " + str(middle_time - start_time))
    join = sort_merge_join(parallel, join, property_tables[LIKES], JOIN_COLUMN1, JOIN_COLUMN2)
    late_time = time.time()
    print("Join 2 finished")
    print("Time for join 2: " + str(late_time - middle_time))
    return sort_merge_join(parallel, join, property_tables[HAS_REVIEW], JOIN_COLUMN1, JOIN_COLUMN2)

string_dict, property_tables = preprocess_data(FILE_PATH)
print("Data preprocessed")
#print(list(property_tables.keys()))

#result_hash_join, time_hash_join = measure_execution_time(request_with_hash_join, property_tables, JOIN_COLUMN1, JOIN_COLUMN2)
#result_hash_join = None
#print("Time hash Join finished!")
#print("Time for time hash join: " + str(time_hash_join))

result_sort_merge_join, time_sort_merge_join = measure_execution_time(request_with_sort_merge_join, False, property_tables, JOIN_COLUMN1, JOIN_COLUMN2)
result_sort_merge_join = None
print("Sort merge join finished!")
print("Time for sort merge join: " + str(time_sort_merge_join))

result_sort_merge_parallel_join, time_sort_merge_parallel_join = measure_execution_time(request_with_sort_merge_join, True, property_tables, JOIN_COLUMN1, JOIN_COLUMN2)
result_sort_merge_parallel_join = None
print("Sort merge join parallel finished!")
print("Time for sort merge parallel join: " + str(time_sort_merge_parallel_join))