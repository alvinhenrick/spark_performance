import pytest

from pyspark.sql import Row

from custom.udf.utils import first_word_udf, first_word_fix_udf


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col="Hello this is my favourite test"),
                                         Row(input_col="This is cool"),
                                         Row(input_col="Time for some performance test"),
                                         Row(input_col="Clarify Tera Team"),
                                         Row(input_col="Doing things right and doing the right thing"),
                                         Row(input_col="Oh Model fit and predict"),
                                         Row(input_col=None),
                                         Row(input_col=None),
                                     ]
                             )

                         ])
def test_udf_call_with_filter(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)
    df = df.withColumn('word', first_word_udf('input_col'))
    df = df.filter("word is not null")

    df.explain(extended=True)

    df.show()


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col="Hello this is my favourite test"),
                                         Row(input_col="This is cool"),
                                         Row(input_col="Time for some performance test"),
                                         Row(input_col="Clarify Tera Team"),
                                         Row(input_col="Doing things right and doing the right thing"),
                                         Row(input_col="Oh Model fit and predict"),
                                         Row(input_col=None),
                                         Row(input_col=None),
                                     ]
                             )

                         ])
def test_udf_call_with_filter_fix(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)
    df = df.withColumn('word2', first_word_fix_udf('input_col'))
    df = df.filter("word2 is not null")

    df.explain(extended=True)

    df.show()
