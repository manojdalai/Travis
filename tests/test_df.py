from pyspark.sql import SparkSession
import pytest
from chispa.column_comparer import *
from chispa.dataframe_comparer import *
from chispa.number_helpers import *
from chispa.row_comparer import *
from chispa.schema_comparer import *



import pyspark.sql.functions as F

spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())


def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")


def test_remove_non_word_characters_short():
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    df1 = (spark.createDataFrame(data, ["name", "expected_name"])
              .withColumn("clean_name", remove_non_word_characters(F.col("name"))))
    assert_column_equality(df1, "clean_name", "expected_name")



