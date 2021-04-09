from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

#sc = SparkContext('local')

spark = SparkSession.builder\
    .master('local[*]')\
    .appName('My App')\
    .getOrCreate()

#spark = SparkSession(sc)
#raw_data = spark.sparkContext.wholeTextFiles("C:\Users\pc\Downloads\dataset_one.csv")

#path = "file:///C:/Users/pc/Downloads/dataset_one.csv"
#path = "C:\\Users\\pc\\Downloads\\dataset_one.csv"

path = "Travis/input/nycflights13.csv"

schema = StructType([
  StructField('id', IntegerType(), True),
  StructField('first_name', StringType(), True),
  StructField('last_name', StringType(), True),
  StructField('email', StringType(), True),
  StructField('country', StringType(), True)
  ])

#raw_data1 = spark.read.csv(path, header=True, mode="DROPMALFORMED", schema=schema)

raw_data1 = spark.read\
    .format('csv')\
    .option('header', 'true')\
    .load(path)

raw_data1.show()