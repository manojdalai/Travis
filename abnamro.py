from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import sys

class pysparkproject:
    print("The script has the name %s" % (sys.argv[1]))
    path = sys.argv[1]

    schema = StructType([
      StructField('id', IntegerType(), True),
      StructField('first_name', StringType(), True),
      StructField('last_name', StringType(), True),
      StructField('email', StringType(), True),
      StructField('country', StringType(), True)
      ])

    spark = SparkSession.builder\
        .master('local[*]')\
        .appName('My App')\
        .getOrCreate()

    def load_dataframe(spark, filename):
        raw_data1 = spark.read \
            .format('csv') \
            .option('header', 'true') \
            .load(filename)
        return raw_data1

    #creating a dataframe
    df_matches = load_dataframe(spark, path)

    df_matches.limit(5).show()




