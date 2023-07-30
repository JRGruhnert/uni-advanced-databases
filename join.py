import numpy as np
from collections import defaultdict
from radix_sort import radix_sort_by_key
from merge_sort import merge_sort, parallel_merge_sort


def hash_join(table1: np.ndarray, table2: np.ndarray, join_key1: int, join_key2: int):
    '''Perform a hash join on two tables.'''

    # Create hash table for table1 using the join_column
    hash_table = defaultdict(list)

    # Hash phase
    for row in table1:
        hash_table[row[join_key1]].append(row)

    # Perform the join
    return [[row1[join_key2],row2[join_key1]] for row2 in table2 for row1 in hash_table[row2[join_key2]]]

   
def sort_merge_join(mode: int, table1: np.ndarray, table2: np.ndarray, join_key1: int, join_key2: int):
    '''Perform a sort-merge join on two tables.'''

    # Sort phase
    sorted_table1 = []
    sorted_table2 = []
    
    if mode == 0:
        sorted_table1 = merge_sort(table1, join_key1)
        sorted_table2 = merge_sort(table2, join_key2)
    elif mode == 1:
        sorted_table1 = parallel_merge_sort(table1, join_key1)
        sorted_table2 = parallel_merge_sort(table2, join_key2)
    else:
        raise ValueError("Invalid mode")
           
    # Perform the join
    idx_left, idx_right = 0, 0
    result = [] 

    while idx_left < len(sorted_table1) and idx_right < len(sorted_table2):
        left_item = sorted_table1[idx_left]
        right_item = sorted_table2[idx_right]

        if left_item[join_key1] == right_item[join_key2]:
            # Join the matching items
            result.append([left_item[join_key1], right_item[join_key2]])
            idx_left += 1
            idx_right += 1
    
        elif left_item[join_key1] < right_item[join_key2]:
            idx_left += 1
        else:
            idx_right += 1

    return np.array(result)





