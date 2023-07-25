from collections import defaultdict

import numpy as np
from radix_sort import radix_sort_by_key
from merge_sort import merge_sort, parallel_merge_sort

# Hash-Join implementation
def hash_join(table1: np.ndarray, table2: np.ndarray, join_key1: int, join_key2: int):
    # Create hash table for table1 using the join_column
    hash_table = defaultdict(list)
    # hash phase
    for row in table1:
        hash_table[row[join_key1]].append(row)

    # Perform the join
    return [[row1[join_key2],row2[join_key1]] for row2 in table2 for row1 in hash_table[row2[join_key2]]]

   

# Sort-Merge-Join implementation
def sort_merge_join(mode: int, table1: np.ndarray, table2: np.ndarray, join_key1: int, join_key2: int):
    # Sort both tables based on the join_column
    sorted_table1 = []
    sorted_table2 = []
    
    if mode == 0:
        sorted_table1 = merge_sort(table1, join_key1)
        sorted_table2 = merge_sort(table2, join_key2)
    elif mode == 1:
        sorted_table1 = radix_sort_by_key(table1, join_key1)
        sorted_table2 = radix_sort_by_key(table2, join_key2)
    elif mode == 2:
        sorted_table1 = parallel_merge_sort(table1, join_key1)
        sorted_table2 = parallel_merge_sort(table2, join_key2)
      

    # Perform the join
    idx_left, idx_right = 0, 0
    #t = 0
    result = [] #np.zeros((len(sorted_table1) + len(sorted_table2), 2))

    while idx_left < len(sorted_table1) and idx_right < len(sorted_table2):
        left_item = sorted_table1[idx_left]
        right_item = sorted_table2[idx_right]

        if left_item[join_key1] == right_item[join_key2]:
            # Join the matching items
            result.append([left_item[join_key1], right_item[join_key2]])
            idx_left += 1
            idx_right += 1
            #t += 1
        elif left_item[join_key1] < right_item[join_key2]:
            idx_left += 1
        else:
            idx_right += 1

    result = np.array(result)
    return result





