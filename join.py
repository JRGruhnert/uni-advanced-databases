from collections import defaultdict
from parallel_sort import merge_sort_threaded

# Hash-Join implementation
def hash_join(table1, table2, join_column1, join_column2):
    # Create hash table for table1 using the join_column
    hash_table = defaultdict(list)
    # hash phase
    for row in table1:
        hash_table[row[join_column1]].append(row)

    # Perform the join
    return [{join_column1: row1[join_column2], join_column2: row2[join_column1]} for row2 in table2 for row1 in hash_table[row2[join_column2]]]

   

# Sort-Merge-Join implementation
def sort_merge_join(parallel: bool, table1, table2, join_column1, join_column2):
    # Sort both tables based on the join_column
    sorted_table1 = []
    sorted_table2 = []
    
    if parallel:
        sorted_table1 = merge_sort_threaded(table1, join_column1)
        sorted_table2 = merge_sort_threaded(table2, join_column2)
    else:
        sorted_table1 = sorted(table1, key=lambda x: x[join_column1])
        sorted_table2 = sorted(table2, key=lambda x: x[join_column2])

    # Perform the join
    result = []
    i, j = 0, 0
    while i < len(sorted_table1) and j < len(sorted_table2):
        if sorted_table1[i][join_column1] == sorted_table2[j][join_column2]:
            # Combine the rows from both tables excluding the join_column
            result.append({**sorted_table1[i], **{k: v for k, v in sorted_table2[j].items() if k != join_column2}})
            j += 1
        elif sorted_table1[i][join_column1] < sorted_table2[j][join_column2]:
            i += 1
        else:
            j += 1

    return result





