from pyspark.sql.functions import expr, udf, pandas_udf, size, split
from pyspark.sql.types import LongType, StringType, StructType, StructField, FloatType
import numpy as np


@udf(returnType=LongType())
def word_count_udf(s):
    """
    Wordcount in sentence native python UDF
    :param s:
    :return:
    """
    return len(s.split(" "))


def example_py_udf(df, out_col, in_col):
    return df.withColumn(out_col, word_count_udf(in_col))


@pandas_udf(returnType=LongType())
def word_count_pandas_udf(s):
    """
     Wordcount in sentence native pandas UDF
    :param s:
    :return:
    """
    return s.str.split(" ").str.len()


def example_vectorized_udf(df, out_col, in_col):
    return df.withColumn(out_col, word_count_pandas_udf(in_col))


def example_scala_udf(df, out_col, in_col):
    """
    Wordcount in sentence scala udf df withColumn
    :param df:
    :param out_col:
    :param in_col:
    :return:
    """
    return df.withColumn(out_col, expr(f"word_count({in_col})"))


def example_built_in_udf(df, out_col, in_col):
    """
    Wordcount in sentence with build in functions withColumn
    :param df:
    :param out_col:
    :param in_col:
    :return:
    """
    return df.withColumn(out_col, size(split(in_col, " ")))


def first_word(s):
    if s is not None:
        return s.split(" ")[0]
    else:
        print("<<<<<<<<<<<<<<<I am null still being invoked>>>>>>>>>>>>>")
        return s


@udf(returnType=StringType())
def first_word_udf(s):
    """
    First word from sentence UDF
    :param s:
    :return:
    """
    return first_word(s)


first_word_fix_udf = udf(lambda s: first_word(s), StringType()).asNondeterministic()


def star_expansion(x):
    sign_x = np.sign(x)
    exp_x = np.exp(x)
    tanh_x = np.tanh(x)
    return sign_x, exp_x, tanh_x


schema = StructType([
    StructField('sign_x', FloatType(), False),
    StructField('exp_x', FloatType(), False),
    StructField('tanh_x', FloatType(), False),
])


@udf(returnType=schema)
def start_expansion_udf(x):
    """
    Start expansion test UDF. Multiple invocation
    :param x:
    :return:
    """
    return star_expansion(x)


start_expansion_fix_udf = udf(lambda s: star_expansion(s), schema).asNondeterministic()
