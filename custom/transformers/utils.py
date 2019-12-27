from custom.udf.utils import example_py_udf, example_scala_udf, example_vectorized_udf, example_built_in_udf
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol

from pyspark.ml.wrapper import JavaTransformer


class MyWordCounterPyUDF(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(MyWordCounterPyUDF, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]
        return example_py_udf(dataset, out_col, in_col)


class MyWordCounterScalaUDF(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(MyWordCounterScalaUDF, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        return example_scala_udf(dataset, out_col, in_col)


class MyWordCounterVectorizedUDF(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(MyWordCounterVectorizedUDF, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        return example_vectorized_udf(dataset, out_col, in_col)


class MyWordCounterBuiltInUDF(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(MyWordCounterBuiltInUDF, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]

        return example_built_in_udf(dataset, out_col, in_col)


class MyWordCountTransformer(JavaTransformer, HasInputCol, HasOutputCol):
    """
    MyWordCountTransformer Custom Scala / Python Wrapper Test
    """

    _classpath = 'com.example.transform.WordCountTransformer'

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(MyWordCountTransformer, self).__init__()
        self._java_obj = self._new_java_obj(
            MyWordCountTransformer._classpath,
            self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        Set the params for the ConfigurableWordCount
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)
