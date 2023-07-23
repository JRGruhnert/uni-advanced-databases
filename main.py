import rdflib
import time
from data import preprocess_data
from join import hash_join, sort_merge_join

FILE_PATH = "/home/jangruhnert/Documents/watdiv.10M/watdiv.10M.nt"

# Measure execution time for a function
# Wraps the function and returns the result and the execution time
def measure_execution_time(func, *args):
    start_time = time.time()
    result = func(*args)
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time

join_column = 'Object'  # Assuming 'Object' column is used for joining
follows = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/follows')
friendOf = rdflib.term.URIRef('http://db.uwaterloo.ca/~galuc/wsdbm/friendOf')
string_dict, property_tables = preprocess_data(FILE_PATH)
print("Data preprocessed")

result_hash_join, time_hash_join = measure_execution_time(hash_join, property_tables[follows], property_tables[friendOf], join_column)
result_hash_join = None
print("Time hash Join finished!")
print("Time for time hash join: " + str(time_hash_join))

result_sort_merge_join, time_sort_merge_join = measure_execution_time(sort_merge_join, False, property_tables[follows], property_tables[friendOf], join_column)
result_sort_merge_join = None
print("Sort merge join finished!")
print("Time for sort merge join: " + str(time_sort_merge_join))

result_sort_merge_parallel_join, time_sort_merge_parallel_join = measure_execution_time(sort_merge_join, True, property_tables[follows], property_tables[friendOf], join_column)
result_sort_merge_parallel_join = None
print("Sort merge join parallel finished!")
print("Time for sort merge parallel join: " + str(time_sort_merge_parallel_join))