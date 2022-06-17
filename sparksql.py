# Belajar sparkSQL
# Import datapasien dari dataset
# Menyajikan statistik data pasien
# Menyajikan rencana perawatan pasien (careplan)



from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# LOAD DATASET==============================================================================================
df = spark\
    .read\
    .format("csv")\
    .options(header='true', inferSchema='true') \
    .load("dataset/csv/patients.csv")

df_careplans = spark\
    .read\
    .format("csv")\
    .options(header='true', inferSchema='true') \
    .load("dataset/csv/careplans.csv")
# DATA EXPLORATION===========================================================================================

df.printSchema()
print('\nSTATISTIK DATA PASIEN')
print('Berdasarkan Etnik')
df.groupBy('ETHNICITY').count().show()
print('\nBerdasarkan Gender')
df.groupBy('GENDER').count().show()
print('\nBerdasarkan Status Pernikahan')
df.groupBy('MARITAL').count().show()
print('\nBerdasarkan Kota')
df.groupBy('COUNTY').count().show()
print('\nBerdasarkan Negara')
df.groupBy('STATE').count().show()


#contoh pengunaan query
print('\nDaftar pasien dengan fasilitas kesehatan')

df.createOrReplaceTempView("patients")
sqlDF = spark.sql("SELECT FIRST, LAST, HEALTHCARE_EXPENSES,HEALTHCARE_COVERAGE FROM patients")
sqlDF.show()

print('\nDaftar rencana perawatan pasien')
df_careplans.createOrReplaceTempView("careplans")
sqlCP = spark.sql("SELECT PATIENT, CODE, DESCRIPTION FROM careplans")
sqlCP.show()
