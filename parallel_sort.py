# Python Program to implement merge sort using
# multi-threading
from multiprocessing import Process
from threading import Thread
 
# number of threads
THREAD_MAX = 8
part = 0

'''
# custom thread
class ListThread(Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)
    def join(self, *args):
        Thread.join(self, *args)
        return self._return
 '''

# merge function for merging two parts
def merge(left, right, join_column):
    l_len = len(left)
    r_len = len(right)
    l_idx = r_idx = 0
    temp = []
 
    # merge left and right in ascending order
    while l_idx < l_len and r_idx < r_len:
        if left[l_idx][join_column] <= right[r_idx][join_column]:
            temp.append(left[l_idx])
            l_idx += 1
        else:
            temp.append(right[r_idx])
            r_idx += 1

    while l_idx < l_len:
        temp.append(left[l_idx])
        l_idx += 1

    while r_idx < r_len:#
        temp.append(right[r_idx])
        r_idx += 1
    return temp

# merge function for merging two parts
def merge_to(result, join_column, low, mid, high):
    left = result[low:mid+1]
    right = result[mid+1:high+1]
    l_len = len(left)
    r_len = len(right)
    l_idx = r_idx = 0
    k = low
 
    # merge left and right in ascending order
    while l_idx < l_len and r_idx < r_len:
        if left[l_idx][join_column] <= right[r_idx][join_column]:
            result[k] = left[l_idx]
            l_idx += 1
        else:
            result[k] = right[r_idx]
            r_idx += 1
        k += 1
    while l_idx < l_len:
        result[k] = left[l_idx]
        l_idx += 1
        k += 1

    while r_idx < r_len:
        result[k] = right[r_idx]
        r_idx += 1
        k += 1

 
# merge sort function
def merge_sort(list, join_column):
    length = len(list)
    if length < 2:
        return list
    
    # calculating mid point of array
    mid = length // 2
    
    left = merge_sort(list[:mid],join_column)
    right = merge_sort(list[mid:],join_column)
    assert len(left) + len(right) == length
    # merging the two halves  
    temp = merge(left, right, join_column)
    assert len(temp) == length
    return temp
    
# merge sort function entry point
def merge_sort_start(list, join_column, result):
    result += merge_sort(list, join_column)
 

# thread function for multi-threading
def merge_sort_threaded(list, join_column):
    length = len(list)
    part_length = length // THREAD_MAX
    rest = length % THREAD_MAX
    result = []
    # creating threads
    for i in range(0, THREAD_MAX):
        lower = i*part_length # lower bound
        upper = (i+1)*part_length # upper bound
        if i == THREAD_MAX - 1: # last thread gets the rest
            upper += rest

        t = Thread(target=merge_sort_start, args=(list[lower:upper], join_column, result))
        t.start()
         
    # joining all threads
    for i in range(THREAD_MAX):
        t.join()

    # merging the final parts
    for i in range(0, THREAD_MAX, 2):
        merge_to(result, join_column, i*part_length, (i+1)*part_length-1,  (i+2)*part_length-1)

    # merging the final parts
    for i in range(0, THREAD_MAX, 4):
        merge_to(result, join_column, i*part_length,  (i+2)*part_length-1, (i+4)*part_length-1)

    merge_to(result, join_column, 0, part_length * THREAD_MAX // 2, part_length * THREAD_MAX-1 + part_length)

    return result
    

 