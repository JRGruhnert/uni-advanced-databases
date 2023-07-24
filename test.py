
import time

import random
from join import hash_join, sort_merge_join

from parallel_sort import merge_sort_threaded, merge_sort_threaded2


randomlist = []
for i in range(10000000):
    #result_str = ''.join(random.choice(string.ascii_lowercase) for i in range(20))
    result_str = random.randint(0, 10000000)
    dic = {'Subject': "doesnt matter", 'Object': result_str}
    randomlist.append(dic)


start_time = time.time()
temp = merge_sort_threaded2(randomlist, 'Object')
end_time = time.time()
print("Time for sort merge parallel join: " + str(end_time - start_time))

start_time2 = time.time()
temp2 = sorted(randomlist, key=lambda x: x['Object'])
end_time2 = time.time()
print("Time for sort merge join: " + str(end_time2 - start_time2))







#list1 = [{'Subject': 'A', 'Object': 2}, {'Subject': 'B', 'Object': 3}, {'Subject': 'C', 'Object': 8}, {'Subject': 'D', 'Object': 13}]
#list2 = [{'Subject': 3, 'Object': 1}, {'Subject': 5, 'Object': 4}, {'Subject': 13, 'Object': 5}, {'Subject': 3, 'Object': 6}]

#propertys = {'A': list1, 'B': list2}
#join = sort_merge_join(False, propertys['A'], propertys['B'], 'Object', 'Subject')
#join2 = hash_join(propertys['A'], propertys['B'], 'Object', 'Subject')
#print(join)
#print(join2)