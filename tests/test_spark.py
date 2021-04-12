import pytest
from ..functions import remove_non_word_characters
#import pyspark.sql.functions as F
#
#
# #def remove_non_word_characters(col):
# #    return F.regexp_replace(col, "[^\\w\\s]+", "")
# from ..functions import remove_non_word_characters
#
#
#
#def test_remove_non_word_characters(spark):
#     data = [
#         ("jo&&se", "jose"),
#         ("**li**", "li"),
#         ("#::luisa", "luisa"),
#         (None, None)
#     ]
#     df = spark.createDataFrame(data, ["name", "expected_name"])\
#         .withColumn("clean_name", remove_non_word_characters(F.col("name")))
#     assert_column_equality(df, "clean_name", "expected_name")

from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())

def test_remove_non_word_characters_long():
    source_data = [
        ("jo&&se",),
        ("**li**",),
        ("#::luisa",),
        (None,)
    ]
    source_df = spark.createDataFrame(source_data, ["name"])

    actual_df = source_df.withColumn(
        "clean_name",
        remove_non_word_characters(F.col("name"))
    )

    expected_data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])

    assert_df_equality(actual_df, expected_df)