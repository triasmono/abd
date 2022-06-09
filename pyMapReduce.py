"""
MapReduce is a data processing job which splits the input data into independent chunks,
which are then processed by the map function and
then reduced by grouping similar sets of the data.

"""
# Load library
import pandas as pd

# Load dataset
df = pd.read_csv('dataset/HepatitisCdata.csv')

"""
Data Exploration

#print(df.shape)
#print(df)
#print(df['Age'])
"""

# Generic Fuction
def multiply(x):
    return (x*x)

def add(x):
    return (x+x)

# MAP
# Mengkonversi seberapa jauh dari tingkat kolesterol aman (6) responden pada dataset
batas_aman = 6
def kolesterol_limit(x):
    return(batas_aman-x)

kolesterol = df['CHOL']
safe_value= map(kolesterol_limit, kolesterol)
print('contoh map ---------- mencari selisih kolesterol dengan batas aman')
print(list(safe_value))

# FILTER
# Menampilikan list usia dibawah 30 tahun yang masuk menjadi responden dalam dataset
age_param = 30
number_list = df['Age']
young = list(filter(lambda x: x < age_param, number_list))
print('contoh filter ---------- menampilkan usia muda yang menjadi responden')
print(young)

#REDUCE
#product = 1
#list = [1, 2, 3, 4]
#for age in list:
#    mean = age * num

from functools import reduce
list_aman = reduce((lambda x, y:x/y), kolesterol)
kolesterol_tertinggi = reduce(lambda a, b: a if a > b else b, kolesterol)

print('contoh reduce ---------- menampilkan kolesterol tertinggi')
print(kolesterol_tertinggi)