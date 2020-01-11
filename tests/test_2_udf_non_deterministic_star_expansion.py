import pytest

from pyspark.sql import Row

from custom.udf.utils import star_expansion_udf, star_expansion_fix_udf
from pyspark.sql.functions import col, explode, array


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col=i) for i in range(50)

                                     ]
                             )

                         ])
def test_udf_star_expansion(spark_session, input_data):
    """
    The problem is UDF gets invoked multiple times for each output. Not good during large scale processing
    :param spark_session:
    :param input_data:
    :return:
    """
    df = spark_session.createDataFrame(input_data)
    result_df = (
        df.select(
            star_expansion_udf(
                col('input_col'),
            ).alias('output_col'))  # invoke UDF
            .select(col('output_col.*'))  # expand into separate columns
    )
    result_df.explain(extended=True)


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col=i) for i in range(50)

                                     ]
                             )

                         ])
def test_udf_star_expansion_fix_1(spark_session, input_data):
    """
    Mark the UDF as asNondeterministic to resolve the issue
    :param spark_session:
    :param input_data:
    :return:
    """
    df = spark_session.createDataFrame(input_data)
    result_df = (
        df.select(
            star_expansion_fix_udf(
                col('input_col'),
            ).alias('output_col2'))  # invoke UDF
            .select(col('output_col2.*'))  # expand into separate columns
    )
    result_df.explain(extended=True)


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col=i) for i in range(50)

                                     ]
                             )

                         ])
def test_udf_star_expansion_fix_2(spark_session, input_data):
    """
    Credit to Schaun Wheeler
    https://towardsdatascience.com/pyspark-udfs-and-star-expansion-b50f501dcb7b
    :param spark_session:
    :param input_data:
    :return:
    """
    df = spark_session.createDataFrame(input_data)
    result_df = (
        df.select(
            explode(
                array(
                    star_expansion_udf(col('input_col'))  # invoke UDF
                )
            ).alias('output_col2')
        ).select(col('output_col2.*'))  # expand into separate columns
    )

    result_df.explain(extended=True)
