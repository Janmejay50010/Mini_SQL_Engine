import re, os

# testdata = [[1,2], [1,2], [2,3]]
# unique_data = [list(x) for x in set(tuple(x) for x in testdata)]

# if re.search('mandy', 'Mandy Pande', re.IGNORECASE):
#     object = re.search('mandy', 'Mandy Pande', re.IGNORECASE)
#     print(object[0])

# print(os.listdir())

test_dict = {}
test_dict['B'] = 1
test_dict['A'] = 2

#test_dict = {'b' if k == 'B' else k:v for k,v in test_dict.items()}
#test_dict['b'] = test_dict.pop('B')
print(test_dict.keys())