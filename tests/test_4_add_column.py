import pytest
from pyspark import Row
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, lit


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col="Hello this is my favourite test"),
                                         Row(input_col="This is cool"),
                                         Row(input_col="Time for some performance test"),
                                         Row(input_col="Clarify Tera Team"),
                                         Row(input_col="Doing things right and doing the right thing"),
                                         Row(input_col="Oh Model fit and predict")
                                     ]
                             )

                         ])
def test_with_select(spark_session: SparkSession, input_data):
    df = spark_session.createDataFrame(input_data)

    new_columns = [lit(None).alias(f"test_{x}") for x in range(100)]
    out_df = df.select([col("*")] + new_columns)

    out_df.explain(extended=True)

    out_df.show()


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col="Hello this is my favourite test"),
                                         Row(input_col="This is cool"),
                                         Row(input_col="Time for some performance test"),
                                         Row(input_col="Clarify Tera Team"),
                                         Row(input_col="Doing things right and doing the right thing"),
                                         Row(input_col="Oh Model fit and predict")
                                     ]
                             )

                         ])
def test_with_column(spark_session: SparkSession, input_data):
    df = spark_session.createDataFrame(input_data)

    out_df = df
    for x in range(100):
        out_df = out_df.withColumn(f"test_{x}", lit(None))

    out_df.explain(extended=True)

    out_df.show()
