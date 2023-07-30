import time
from data import FOLLOWS_10M, FOLLOWS_100K, FRIEND_OF_10M, FRIEND_OF_100K, HAS_REVIEW_10M, HAS_REVIEW_100K, LIKES_10M, LIKES_100K, preprocess_data
from join import hash_join, sort_merge_join

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

def request_with_hash_join(property_tables, format):
    if format == "nt":
        join = hash_join(property_tables[FOLLOWS_10M], property_tables[FRIEND_OF_10M], JOIN_COLUMN1, JOIN_COLUMN2)
        join = hash_join(join, property_tables[LIKES_10M], JOIN_COLUMN1, JOIN_COLUMN2)
        return hash_join(join, property_tables[HAS_REVIEW_10M], JOIN_COLUMN1, JOIN_COLUMN2)
    elif format == "txt":
        join = hash_join(property_tables[FOLLOWS_100K], property_tables[FRIEND_OF_100K], JOIN_COLUMN1, JOIN_COLUMN2)
        join = hash_join(join, property_tables[LIKES_100K], JOIN_COLUMN1, JOIN_COLUMN2)
        return hash_join(join, property_tables[HAS_REVIEW_100K], JOIN_COLUMN1, JOIN_COLUMN2)
    else:
        print("Wrong format")
        return None

def request_with_sort_merge_join(parallel, property_tables, format):
    if format == "nt":
        join = sort_merge_join(parallel, property_tables[FOLLOWS_10M], property_tables[FRIEND_OF_10M], JOIN_COLUMN1, JOIN_COLUMN2)
        join = sort_merge_join(parallel, join, property_tables[LIKES_10M], JOIN_COLUMN1, JOIN_COLUMN2)
        return sort_merge_join(parallel, join, property_tables[HAS_REVIEW_10M], JOIN_COLUMN1, JOIN_COLUMN2)
    elif format == "txt":
        join = sort_merge_join(parallel, property_tables[FOLLOWS_100K], property_tables[FRIEND_OF_100K], JOIN_COLUMN1, JOIN_COLUMN2)
        join = sort_merge_join(parallel, join, property_tables[LIKES_100K], JOIN_COLUMN1, JOIN_COLUMN2)
        return sort_merge_join(parallel, join, property_tables[HAS_REVIEW_100K], JOIN_COLUMN1, JOIN_COLUMN2)
    else:
        print("Wrong format")
        return None

dict_100k, tables_100k = preprocess_data("txt")
print("Data preprocessed")


result_hash_join, time_hash_join = measure_execution_time(request_with_hash_join, tables_100k, "txt")
result_hash_join = None
print("Time hash Join finished!")
print("Time for time hash join: " + str(time_hash_join))

result_sort_merge_join, time_sort_merge_join = measure_execution_time(request_with_sort_merge_join, 0, tables_100k, "txt")
result_sort_merge_join = None
print("Sort merge join finished!")
print("Time for sort merge join: " + str(time_sort_merge_join))

result_sort_merge_parallel_join, time_sort_merge_parallel_join = measure_execution_time(request_with_sort_merge_join, 1, tables_100k, "txt")
result_sort_merge_parallel_join = None
print("Sort merge join parallel finished!")
print("Time for sort merge parallel join: " + str(time_sort_merge_parallel_join))

dict_10m, tables_10m = preprocess_data("nt")
print("Data preprocessed")

result_hash_join, time_hash_join = measure_execution_time(request_with_hash_join, tables_10m, "nt")
result_hash_join = None
print("Time hash Join finished!")
print("Time for time hash join: " + str(time_hash_join))

result_sort_merge_join, time_sort_merge_join = measure_execution_time(request_with_sort_merge_join, 0, tables_10m, "nt")
result_sort_merge_join = None
print("Sort merge join finished!")
print("Time for sort merge join: " + str(time_sort_merge_join))

result_sort_merge_parallel_join, time_sort_merge_parallel_join = measure_execution_time(request_with_sort_merge_join, 1, tables_10m, "nt")
result_sort_merge_parallel_join = None
print("Sort merge join parallel finished!")
print("Time for sort merge parallel join: " + str(time_sort_merge_parallel_join))

