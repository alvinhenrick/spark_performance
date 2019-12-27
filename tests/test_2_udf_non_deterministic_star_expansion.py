import pytest

from pyspark.sql import Row

from custom.udf.utils import start_expansion_udf, start_expansion_fix_udf
from pyspark.sql.functions import col


@pytest.mark.parametrize('input_data',
                         [
                             (
                                     [
                                         Row(input_col=i) for i in range(50)

                                     ]
                             )

                         ])
def test_udf_star_expansion(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)
    result_df = (
        df.select(
            start_expansion_udf(
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
def test_udf_star_expansion_fix(spark_session, input_data):
    df = spark_session.createDataFrame(input_data)
    result_df = (
        df.select(
            start_expansion_fix_udf(
                col('input_col'),
            ).alias('output_col2'))  # invoke UDF
            .select(col('output_col2.*'))  # expand into separate columns
    )
    result_df.explain(extended=True)
