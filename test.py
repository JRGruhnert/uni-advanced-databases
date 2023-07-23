


import random

from parallel_sort import merge_sort_threaded


randomlist = []
for i in range(10000):
    #result_str = ''.join(random.choice(string.ascii_lowercase) for i in range(20))
    result_str = random.randint(0, 1000000)
    dic = {'Subject': "doesnt matter", 'Object': result_str}
    randomlist.append(dic)

print("randomlist bevor")
print(len(randomlist))
temp = merge_sort_threaded(randomlist, 'Object')
print("randomlist")
print(len(temp))
for i in temp[0:100]:
    print(i['Object'])



'''
list1 = [{'Subject': 'A', 'Object': 2}, {'Subject': 'B', 'Object': 3}, {'Subject': 'C', 'Object': 8}, {'Subject': 'D', 'Object': 13}]
list2 = [{'Subject': 'A', 'Object': 1}, {'Subject': 'B', 'Object': 4}, {'Subject': 'C', 'Object': 5}, {'Subject': 'D', 'Object': 6}]
merged = merge(list1, list2, "Object")
print(len(merged))
for i in merged:
    print(i['Object'])
'''