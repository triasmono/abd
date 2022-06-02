import pandas as pd

df = pd.read_csv('dataset/HepatitisCdata.csv')
print(df.shape)


""" tentukan dulu studi kasusnya
- map untuk mengubah data hasil hasilnya n to n
- filter untuk mengubah data list hasilnya n to m dengna n> m (reduce)
- reduce untuk mengubah data list hasilnya n to 1

studi kasus dari data hepa?
- 

"""

#map
def multiply(x):
    return (x*x)
def add(x):
    return (x+x)

funcs = [multiply, add]
for i in range(5):
    value = list(map(lambda x: x(i), funcs))
    print(value)

#filter
number_list = range(-5, 5)
less_than_zero = list(filter(lambda x: x < 0, number_list))
print(less_than_zero)


#reduce
product = 1
list = [1, 2, 3, 4]
for num in list:
    product = product * num

from functools import reduce
product = reduce((lambda x, y: x * y), [1, 2, 3, 4])

# Output: 24