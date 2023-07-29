import time
from data import FOLLOWS, FOLLOWS2, FRIEND_OF, FRIEND_OF2, HAS_REVIEW, HAS_REVIEW2, LIKES, LIKES2, preprocess_data
from join import hash_join, sort_merge_join

#FILE_PATH = "/home/jangruhnert/Documents/watdiv.10M/watdiv.10M.nt"
FILE_PATH = "/home/jangruhnert/Documents/watdiv.10M/100k.txt"
JOIN_COLUMN1 = 0  # Assuming 'Object' column is used for joining
JOIN_COLUMN2 = 1  # Assuming 'Subject' column is used for joining

# Measure execution time for a function
# Wraps the function and returns the result and the execution time
def measure_execution_time(func, *args):
    start_time = time.time()
    result = func(*args)
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time

def request_with_hash_join(property_tables, join_column1, join_column2):
    join = hash_join(property_tables[FOLLOWS2], property_tables[FRIEND_OF2], JOIN_COLUMN1, JOIN_COLUMN2)
    join = hash_join(join, property_tables[LIKES2], JOIN_COLUMN1, JOIN_COLUMN2)
    return hash_join(join, property_tables[HAS_REVIEW2], JOIN_COLUMN1, JOIN_COLUMN2)

def request_with_sort_merge_join(parallel, property_tables, join_column1, join_column2):
    join = sort_merge_join(parallel, property_tables[FOLLOWS2], property_tables[FRIEND_OF2], JOIN_COLUMN1, JOIN_COLUMN2)
    join = sort_merge_join(parallel, join, property_tables[LIKES2], JOIN_COLUMN1, JOIN_COLUMN2)
    return sort_merge_join(parallel, join, property_tables[HAS_REVIEW2], JOIN_COLUMN1, JOIN_COLUMN2)

string_dict, property_tables = preprocess_data(FILE_PATH, "txt")
print("Data preprocessed")


result_hash_join, time_hash_join = measure_execution_time(request_with_hash_join, property_tables, JOIN_COLUMN1, JOIN_COLUMN2)
result_hash_join = None
print("Time hash Join finished!")
print("Time for time hash join: " + str(time_hash_join))

result_sort_merge_join, time_sort_merge_join = measure_execution_time(request_with_sort_merge_join, 0, property_tables, JOIN_COLUMN1, JOIN_COLUMN2)
result_sort_merge_join = None
print("Sort merge join finished!")
print("Time for sort merge join: " + str(time_sort_merge_join))

result_sort_merge_parallel_join, time_sort_merge_parallel_join = measure_execution_time(request_with_sort_merge_join, 2, property_tables, JOIN_COLUMN1, JOIN_COLUMN2)
result_sort_merge_parallel_join = None
print("Sort merge join parallel finished!")
print("Time for sort merge parallel join: " + str(time_sort_merge_parallel_join))

#result_sort_merge_radix_join, time_sort_merge_radix_join = measure_execution_time(request_with_sort_merge_join, 1, property_tables, JOIN_COLUMN1, JOIN_COLUMN2)
#result_sort_merge_radix_join = None
#print("Sort merge join radix finished!")
#print("Time for sort merge radix join: " + str(time_sort_merge_radix_join))
