import pytest

from custom.transformers.utils import MyWordCounterPyUDF, MyWordCounterScalaUDF, MyWordCounterVectorizedUDF, \
    MyWordCounterBuiltInUDF
from pyspark.sql import Row


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
def test_py_udf_transformer(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)

    tr = MyWordCounterPyUDF(inputCol="input_col", outputCol="output_col")

    tdf = tr.transform(df)

    tdf.explain()

    tdf.show()

    assert tdf.count() == 6


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
def test_scala_udf_transformer(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)

    tr = MyWordCounterScalaUDF(inputCol="input_col", outputCol="output_col")

    tdf = tr.transform(df)

    tdf.explain()

    tdf.show()

    assert tdf.count() == 6


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
def test_vectorized_udf_transformer(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)

    tr = MyWordCounterVectorizedUDF(inputCol="input_col", outputCol="output_col")

    tdf = tr.transform(df)

    tdf.explain()

    tdf.show()

    assert tdf.count() == 6


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
def test_built_in_udf_transformer(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)

    tr = MyWordCounterBuiltInUDF(inputCol="input_col", outputCol="output_col")

    tdf = tr.transform(df)

    tdf.explain()

    tdf.show()

    assert tdf.count() == 6
