#https://www.analyticsvidhya.com/blog/2019/11/build-machine-learning-pipelines-pyspark/
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("sample").getOrCreate()
#spark = SparkSession.builder.config('spark.port.maxRetries', 100).getOrCreate()

#spark.version

df = spark\
    .read\
    .format("csv")\
    .options(header='true', inferSchema='true')\
    .load("hdfs://localhost:8020/user/triasmono/HepatitisCdata.csv")

#df.show()
df.printSchema()
#print(df['Age'])

# Data Exploration (check per label)
df.groupBy('Category').count().show()

#Check kolom data
print(df.columns)

#kalkulasi dimensi data
print('Jumlah row: '+str(df.count())+'\nJumlah kolom: '+str(len(df.columns)))

# get the summary of the numerical columns
df.select('Category', 'Age', 'Sex').describe().show()

# missing value count
# import sql function pyspark

# null values in each column
data_agg = df.agg(*[f.count(f.when(f.isnull(c), c)).alias(c) for c in df.columns])
data_agg.show()

# Indexing - convert categorical to numerical

from pyspark.ml.feature import StringIndexer

# create object of StringIndexer class and specify input and output column
SI_category = StringIndexer(inputCol='Category',outputCol='Category_Index')
SI_sex = StringIndexer(inputCol='Sex',outputCol='Sex_Index')

# transform the data
df = SI_category.fit(df).transform(df)
df = SI_sex.fit(df).transform(df)

# view the transformed data
df.select('Category', 'Category_Index', 'Sex', 'Sex_Index').show(10)