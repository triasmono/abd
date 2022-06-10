# Learning how spark work
# Create model untuk

#https://www.analyticsvidhya.com/blog/2019/11/build-machine-learning-pipelines-pyspark/
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("sample").getOrCreate()
#spark = SparkSession.builder.config('spark.port.maxRetries', 100).getOrCreate()
#spark.version

#LOAD DATASET
df = spark\
    .read\
    .format("csv")\
    .options(header='true', inferSchema='true') \
    .load("dataset/HepatitisCdata.csv")

# load from HDFS
#    .load("hdfs://localhost:8020/user/triasmono/HepatitisCdata.csv")

#df.show()

#DATA EXPLORATION
df.printSchema()
# Data Exploration (check per label)
df.groupBy('Category').count().show()
print('Jumlah row: '+str(df.count())+'\nJumlah kolom: '+str(len(df.columns)))
# get the summary of the numerical columns
#df.select("Age","ALB","ALP","ALT","AST","BIL","CHE","CHOL","CREA","GGT","PROT").describe().show()

# drop the columns that are not required
#df = df.drop(*["_c0","ALB","ALP","ALT","AST","CHE","CHOL","CREA","GGT","PROT"])
df = df.drop(*["_c0"])
df.printSchema()

# missing value count
# null values in each column
data_agg = df.agg(*[f.count(f.when(f.isnull(c), c)).alias(c) for c in df.columns])
data_agg.show()



# Machine learning
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# INDEXING - convert categorical collumn to numerical
stage_1 = StringIndexer(inputCol='Category', outputCol='cat_index')
stage_2 = StringIndexer(inputCol='Sex', outputCol='sex_index')
stage_3 = StringIndexer(inputCol='ALB', outputCol='alb_index')
stage_4 = StringIndexer(inputCol='ALP', outputCol='alp_index')
stage_5 = StringIndexer(inputCol='ALT', outputCol='alt_index')
stage_6 = StringIndexer(inputCol='CHOL', outputCol='chol_index')
stage_7 = StringIndexer(inputCol='PROT', outputCol='prot_index')

# define stage 3: one hot encode the numeric versions of feature 2 and 3 generated from stage 1 and stage 2
stage_8 = OneHotEncoder(inputCols=[stage_1.getOutputCol(),
                                   stage_2.getOutputCol(),
                                   stage_3.getOutputCol(),
                                   stage_4.getOutputCol(),
                                   stage_5.getOutputCol(),
                                   stage_6.getOutputCol(),
                                   stage_7.getOutputCol()],
                        outputCols=['cat_encoded',
                                    'sex_encoded',
                                    'alb_encoded',
                                    'alp_encoded',
                                    'alt_encoded',
                                    'chol_encoded',
                                    'prot_encoded'])

# define stage 4: create a vector of all the features required to train the logistic regression model
stage_9 = VectorAssembler(inputCols=['Age','BIL','CHE','CREA','GGT', 'sex_encoded',
                                     'alb_encoded', 'alt_encoded', 'chol_encoded','prot_encoded'],
                          outputCol='features')

# define stage 5: logistic regression model
stage_10 = LogisticRegression(featuresCol='features',labelCol='cat_index')

# setup the pipeline
regression_pipeline = Pipeline(stages= [stage_1, stage_2, stage_3, stage_4,
                                        stage_5, stage_6, stage_7, stage_8,
                                        stage_9, stage_10])

df = df.fillna(0)
# fit the pipeline for the trainind data
model = regression_pipeline.fit(df)
# transform the data
train_data = model.transform(df)

# view some of the columns generated
train_data.select('features', 'Category', 'rawPrediction', 'probability', 'prediction').show()

#PREDICTION
# create a sample data without the labels
"""
sample_data_test = spark.createDataFrame([
    (32, "m", 38.5, 52.5, 7.7, 22.1, 7.5, 6.93, 3.23, 106, 12.1, 69),
    (32, "m", 38.5, 70.3, 18, 24.7, 3.9, 11.17, 4.8, 74, 15.6, 76.5),
    (32, "m", 46.9, 74.7, 36.2, 52.6, 6.1, 8.84, 5.2, 86, 33.2, 79.3),
    (32, "m", 43.2, 52, 30.6, 22.6, 18.9, 7.33, 4.74, 80, 33.8, 75.7),
    (32, "m", 39.2, 74.1, 32.6, 24.8, 9.6, 9.15, 4.32, 76, 29.9, 68.7),
    (32, "m", 41.6, 43.3, 18.5, 19.7, 12.3, 9.92, 6.05, 111, 91, 74),
    (32, "m", 46.3, 41.3, 17.5, 17.8, 8.5, 7.01, 4.79, 70, 16.9, 74.5),
    (32, "m", 42.2, 41.9, 35.8, 31.1, 16.1, 5.82, 4.6, 109, 21.5, 67.1),
    (32, "m", 50.9, 65.5, 23.2, 21.2, 6.9, 8.69, 4.1, 83, 13.7, 71.3),
    (32, "m", 42.4, 86.3, 20.3, 20, 35.2, 5.46, 4.45, 81, 15.9, 69.9)],
    ["Age","Sex","ALB","ALP","ALT","AST","BIL","CHE","CHOL","CREA","GGT","PROT"])
"""
# fit the pipeline for the trainind data
#model = regression_pipeline.fit(sample_data_test)

# transform the data using the pipeline
#sample_data_test = model.transform(sample_data_test)

# see the prediction on the test data
#sample_data_test.select('features', 'rawPrediction', 'probability', 'prediction').show()