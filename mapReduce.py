# Reference : https://medium.com/geekculture/mapreduce-with-python-5d12a772d5b3
# run : python mapReduce.py dataset/HepatitisCdata.csv

# run with hadoop : python [python file] -r hadoop --hadoop-streaming-jar [The_path_of_Hadoop_Streaming_jar] [dataset]

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

# split data by ,
columns = 'Category,Age'.split(',')
class MapReduce(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_ages,
                  reducer=self.reducer_count_ages)
        ]
#Mapper function
    def mapper_get_ages(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           age=diction['Age']
           #outputing as key value pairs
           yield age, 1

#Reducer function
    def reducer_count_ages(self, key, values):
       yield key, sum(values)

if __name__ == "__main__":
    MapReduce.run()