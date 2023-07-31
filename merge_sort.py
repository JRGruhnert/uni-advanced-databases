from multiprocessing import Manager, Process, Queue
import numpy as np

class Counter:
    def __init__(self):
        self.value = 0
    def increment(self):
        self.value += 1
    def get_value(self):
        return self.value
    def reset(self):
        self.value = 0


move_counter = Counter()

def merge_sort_multiple(results: Queue, array: np.ndarray, join_key: int):
  '''Sort the array using merge sort.'''
  results.put(merge_sort(array, join_key))


def merge_multiple(results: Queue, array_part_left: np.ndarray, array_part_right: np.ndarray, join_key: int):
  '''Merge two sorted lists into a single sorted list.'''
  results.put(merge(array_part_left, array_part_right, join_key))


def merge_sort(array: np.ndarray, join_key: int):
    '''Sort the array using merge sort.'''
    
    array_length = len(array)

    if array_length <= 1:
        return array

    middle_index = array_length // 2

    left = array[0:middle_index]
    right = array[middle_index:]
    left = merge_sort(left, join_key)
    right = merge_sort(right, join_key)
    return merge(left, right, join_key)


def merge(left: np.ndarray, right: np.ndarray, join_key: int):
    '''Merge two sorted lists into a single sorted list.'''

    sorted_list = np.zeros((len(left) + len(right), 2), dtype=int)

    idx_sorted = 0
    idx_left = 0
    idx_right = 0
    max_left = len(left)
    max_right = len(right)

    while idx_left < max_left and idx_right < max_right:
        if left[idx_left][join_key] <= right[idx_right][join_key]:
            sorted_list[idx_sorted] = left[idx_left]
            idx_left += 1
            idx_sorted += 1
        else:
            sorted_list[idx_sorted] = right[idx_right]
            idx_right += 1
            idx_sorted += 1
    if idx_left < max_left:
            sorted_list[idx_sorted:] = left[idx_left:]
    if idx_right < max_right:
            sorted_list[idx_sorted:] = right[idx_right:]

    return sorted_list

def parallel_merge_sort(array: np.ndarray, join_key: int, process_count=8):
    '''Sort the array using merge sort and parallelism.'''

    batch_size = int((len(array) - 1) / process_count) # Size of each batch
    results_queue = Manager().Queue() # Queue to store the results of the merge sort processes
    processes = [] # List to store the processes

    for n in range(process_count):
        if n < process_count - 1:
            batch = array[n * batch_size:(n + 1) * batch_size]
        else:
            batch = array[n * batch_size:] #last batch may be larger
        
        # Start a new process to sort the batch
        process = Process(target=merge_sort_multiple, args=(results_queue, batch, join_key))
        process.start()
        processes.append(process)

    # Wait for all processes to finish
    for process in processes:
        process.join()

    # Merge the results in a binary tree fashion
    while results_queue.qsize() > 1:
        # Create a list to hold the merge processes
        merge_processes = []
        for i in range(0, results_queue.qsize(), 2):
            process = Process(target=merge_multiple, args=(results_queue, results_queue.get(), results_queue.get(), join_key))
            process.start()
            merge_processes.append(process)

        # Wait for all merge processes to finish
        for process in merge_processes:
            process.join()

    return results_queue.get()
