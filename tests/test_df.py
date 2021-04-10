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
    print(assert_column_equality(df1, "clean_name", "expected_name"))

def test_remove_non_word_characters_nice_error():
    data = [
        ("matt7", "matt"),
        ("bill&", "bill"),
        ("isabela*", "isabela"),
        (None, None)
    ]
    df = (spark.createDataFrame(data, ["name", "expected_name"])
          .withColumn("clean_name", remove_non_word_characters(F.col("name"))))
    print(assert_column_equality(df, "clean_name", "expected_name"))

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

