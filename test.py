import string
import time
import random

import numpy as np
from join import hash_join, sort_merge_join
from radix_sort import radix_sort_by_key
from merge_sort import merge_sort, parallel_merge_sort


randomlist = np.random.randint(low=0, high=1000000, size=(100000, 2))
randomlist2 = np.random.randint(low=0, high=1000000, size=(100000, 2))


print("Lists created")

#start_time = time.time()
#temp = radix_sort_by_key(randomlist, 'Object')
#end_time = time.time()
#print("Time for radix sort: " + str(end_time - start_time))

#start_time2 = time.time()
#temp2 = merge_sort(randomlist, 1)
#end_time2 = time.time()
#print("Time for merge sort: " + str(end_time2 - start_time2))

#start_time3 = time.time()
#temp3 = parallel_merge_sort(randomlist, 'Object')
#end_time3 = time.time()
#print("Time for parallel merge sort: " + str(end_time3 - start_time3)
#start_time4 = time.time()
#join = hash_join(randomlist, randomlist2, 'Object', 'Subject')
#end_time4 = time.time()
#print("Time for hash join: " + str(end_time4 - start_time4))

start_time5 = time.time()
join2 = sort_merge_join(False, randomlist, randomlist2, 1, 0)
end_time5 = time.time()
print("Time for sort merge join: " + str(end_time5 - start_time5))

#start_time6 = time.time()
#join3 = sort_merge_join(True, randomlist, randomlist2, 'Object', 'Subject')
#end_time6 = time.time()
#print("Time for radix sort merge join: " + str(end_time6 - start_time6))


#list1 = [{'Subject': 'A', 'Object': 2}, {'Subject': 'B', 'Object': 3}, {'Subject': 'C', 'Object': 8}, {'Subject': 'D', 'Object': 13}]
#list2 = [{'Subject': 3, 'Object': 1}, {'Subject': 5, 'Object': 4}, {'Subject': 13, 'Object': 5}, {'Subject': 3, 'Object': 6}]

#propertys = {'A': list1, 'B': list2}
#join = sort_merge_join(False, propertys['A'], propertys['B'], 'Object', 'Subject')
#join2 = hash_join(propertys['A'], propertys['B'], 'Object', 'Subject')
#print(join)
#print(join2)