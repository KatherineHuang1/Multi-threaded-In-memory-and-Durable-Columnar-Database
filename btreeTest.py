from BTrees.OOBTree import OOBTree

k = OOBTree()
t = OOBTree()
# t.update({1: "red", 2: "green", 3: "blue", 4: "spades"})
'''
location1 = ('a', 1)
location2 = ('b', 2)
location3 = ('c', 3)
location4 = ('o', 1)
location5 = ('l', 3)
location6 = ('w', 8)
'''
count = 1

#list1 = [('a', count), ('p', count), ('a', count), ('c', count), ('a', count), ('c', count)]
list1 = [("ai", 1), ("pi", 1), ("ai", 3), ("ci", 3), ("ai", 1), ("ci", 3), ("ai", 1)]
list6 = [(11, 1), (122222, 1), (13, 1), (111222, 3), (12, 1), (14, 1), (16, 1)]
list2 = [1, 2, 3, 4, 5, 6, 7]
list_repeat = {}
list_rid = []

'''
key1 = ('a', count)
key2 = ('p', count)
key3 = ('k', count)
key4 = ('m', count)
key5 = ('b', count)
key6 = ('c', count)
'''

'''
for i in range(len(list1)):
    if t.insert(list1[i], list2[i]) == 0:
        if list1[i][0] in list_repeat.keys():

            count2 = list_repeat[list1[i][0]] + 1
            list1[i] = (list1[i][0], count2)
            t.insert(list1[i], list2[i])
            list_repeat[list1[i][0]] = count2

        else:
            count1 = list1[i][1] + 1
            list1[i] = (list1[i][0], count1)
            list_repeat[list1[i][0]] = list1[i][1]
            t.insert(list1[i], list2[i])
'''
'''
for i in range(len(list1)):
    if t.insert(list1[i], list2[i]) == 0:
        value = t.pop(list1[i])
       # print("in the loop: ", value)
       # print("len: ", len(value[0]))
        if len(str(value[0])) == 1:
            list_rid.append(value)
        elif len(value[0]) > 1:
            for j in range(len(value)):
                list_rid.append(value[j])
        list_rid.append(list2[i])
        t.insert(list1[i], list_rid)
        list_rid = []
'''
'''    
    else:

        #print("in else list_rid: ", list_rid)
        t.insert(list1[i], list2[i])
        list_rid = []
'''

for i in range(len(list1)):
    list10 = []
    list10.append(list2[i])
    if t.insert(list1[i], list10) == 0:
        value = t.pop(list1[i])
        print("in the loop: ", value)
       # print("len: ", len(value[0]))
        '''
        if len(str(value)) == 1:
            list_rid.append(value)
        elif len(str(value)) > 1:
            for j in range(len(value)):
                list_rid.append(value[j])
        '''
        value.append(list2[i])
        #list_rid.append(list2[i])
        t.insert(list1[i], value)
       # list_rid = []



for j in range(len(list6)):
    print(list6[j])
    list10 = []
    list10.append(list2[j])
    print(list10)
    if k.insert(list6[j], list10) == 0:
        print("k")
        value = k.pop(list6[j])
        print("in the loop2: ", value)
       # print("len: ", len(value[0]))
        '''
        if len(str(value)) == 1:
            list_rid.append(value)
        elif len(str(value)) > 1:
            for j in range(len(value)):
                list_rid.append(value[j])
        '''
        value.append(list2[j])
        #list_rid.append(list2[i])
        k.insert(list6[j], value)



'''
t.insert('a', location1)
t.insert('c', location2)
t.insert('d', location3)
t.insert('b', location4)
#t.insert(('y', 3), 10)

'''
s = t.keys()
d = t.values()

#t.__delitem__('ai')

#t.__setitem__(location1, 'y')

'''
if t.has_key(("zi", 3)):
    print("today")
else:
    print("yesterday")

list3 = []
list3 = t.get(("ai", 1), -1)


print(list3)

for i in range(len(list3)):
    print("get element from the key ", list3[i])

for key in t.keys():
    print("the column is:", key[1])

#print("check insert", t.insert('e', ('p', 2)))

print("the value of key ", t.get(("ai", 3)))
#list6 = [lis[1] for lis in t.get(("ai", 1))]
#print("get tree [1]: ", list6)
print("keys: ", list(t.keys()))
print("values: ", list(d))


print(len(t))
print("hello")
'''
print("keys ", list(k.keys()))
print("value ", list(k.values()))

'''
result = filter(lambda x: (11, 1) <= x <= (16, 1), list(k.keys()))
return_result = list(result)
print("the key in the range: ", return_result)
list8 = []

for i in range(len(return_result)):
    print("i ", return_result[i])
    res = int("". join(map(str, k.get(return_result[i]))))
    list8.append(res)
'''

list8 = []

x = 11
y = 16
c = 1
if k.has_key((11, 1)):
    print("today")
else:
    print("yesterday")

while x <= y:
    new_key = (x, c)
    #print("x:", x)
    #print(new_key)
    if k.has_key(new_key):
        print(new_key, k.get(new_key))
        res = int("". join(map(str, k.get(new_key))))
        list8.append(res)
        x = x+1
    else:
        x = x+1

print("rid in the range: ", list8)
pass
