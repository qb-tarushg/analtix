from transformation import Metrics
from pyspark.sql.functions import count, min, max, avg, sum, countDistinct, mean, sumDistinct


class FeatureOps(object):
    """
    TODO use factory and builder design pattern.
    """

    def __init__(self, data, on_cols, dimensions):
        self.data = data
        self.on_cols = on_cols
        self.dim = dimensions

    def compute(self):
        pass


class CountFeatureOps(FeatureOps):
    """
    This class represent the feature transformation operation of type `count`.
    """

    def __init__(self, data, on_cols, dimensions):
        super().__init__(data=data, on_cols=on_cols, dimensions=dimensions)

    def compute(self):
        """
        This method computes the count for the required feature!
        :return: DataFrame (spark)
        """
        assert self.dim, "Dimensions/granularity is not defined!"
        assert self.on_cols, "No columns passed for aggregate counting!"
        unpacking, expr = _get_expression(Metrics.COUNT, cols=self.on_cols, func=count)

        return self.data.groupBy(self.dim).agg(*expr) if unpacking else self.data.groupBy(self.dim).agg(expr)


class MaxFeatureOps(FeatureOps):

    def __init__(self, data, on_cols, dimensions):
        super().__init__(data=data, on_cols=on_cols, dimensions=dimensions)

    def compute(self):
        assert self.dim, "Dimensions/granularity is not defined!"
        assert self.on_cols, "No columns passed for aggregate counting!"
        unpacking, expr = _get_expression(Metrics.MAX, cols=self.on_cols, func=max)

        return self.data.groupBy(self.dim).agg(*expr) if unpacking else self.data.groupBy(self.dim).agg(expr)


class MinFeatureOps(FeatureOps):
    def __init__(self, data, on_cols, dimensions):
        super().__init__(data=data, on_cols=on_cols, dimensions=dimensions)

    def compute(self):
        assert self.dim, "Dimensions/granularity is not defined!"
        assert self.on_cols, "No columns passed for aggregate counting!"
        unpacking, expr = _get_expression(Metrics.MIN, cols=self.on_cols, func=min)

        return self.data.groupBy(self.dim).agg(*expr) if unpacking else self.data.groupBy(self.dim).agg(expr)


class AvgFeatureOps(FeatureOps):
    def __init__(self, data, on_cols, dimensions):
        super().__init__(data=data, on_cols=on_cols, dimensions=dimensions)

    def compute(self):
        assert self.dim, "Dimensions/granularity is not defined!"
        assert self.on_cols, "No columns passed for aggregate counting!"
        unpacking, expr = _get_expression(Metrics.AVG, cols=self.on_cols, func=avg)

        return self.data.groupBy(self.dim).agg(*expr) if unpacking else self.data.groupBy(self.dim).agg(expr)


class SumFeatureOps(FeatureOps):
    def __init__(self, data, on_cols, dimensions):
        super().__init__(data=data, on_cols=on_cols, dimensions=dimensions)

    def compute(self):
        assert self.dim, "Dimensions/granularity is not defined!"
        assert self.on_cols, "No columns passed for aggregate counting!"
        unpacking, expr = _get_expression(Metrics.SUM, cols=self.on_cols, func=sum)

        return self.data.groupBy(self.dim).agg(*expr) if unpacking else self.data.groupBy(self.dim).agg(expr)


class CountDistinctFeatureOps(FeatureOps):
    def __init__(self, data, on_cols, dimensions):
        super().__init__(data=data, on_cols=on_cols, dimensions=dimensions)

    def compute(self):
        assert self.dim, "Dimensions/granularity is not defined!"
        assert self.on_cols, "No columns passed for aggregate counting!"
        unpacking, expr = _get_expression(Metrics.COUNT_DISTINCT, cols=self.on_cols, func=countDistinct)

        return self.data.groupBy(self.dim).agg(*expr) if unpacking else self.data.groupBy(self.dim).agg(expr)


class SumDistinctFeatureOps(FeatureOps):
    def __init__(self, data, on_cols, dimensions):
        super().__init__(data=data, on_cols=on_cols, dimensions=dimensions)

    def compute(self):
        assert self.dim, "Dimensions/granularity is not defined!"
        assert self.on_cols, "No columns passed for aggregate counting!"
        unpacking, expr = _get_expression(Metrics.SUM_DISTINCT, cols=self.on_cols, func=sumDistinct)

        return self.data.groupBy(self.dim).agg(*expr) if unpacking else self.data.groupBy(self.dim).agg(expr)


class MeanFeatureOps(FeatureOps):
    def __init__(self, data, on_cols, dimensions):
        super().__init__(data=data, on_cols=on_cols, dimensions=dimensions)

    def compute(self):
        assert self.dim, "Dimensions/granularity is not defined!"
        assert self.on_cols, "No columns passed for aggregate counting!"
        unpacking, expr = _get_expression(Metrics.MEAN, cols=self.on_cols, func=mean)

        return self.data.groupBy(self.dim).agg(*expr) if unpacking else self.data.groupBy(self.dim).agg(expr)


def _get_expression(metric, cols, func):
    unpacking = False
    if isinstance(cols, dict):
        expr = [func(col).alias(alias) for col, alias in cols.items()]
        unpacking = True
    elif isinstance(cols, list):
        expr = {el: metric.value for el in cols}
    elif isinstance(cols, str):
        expr = func(cols)
    else:
        raise AttributeError(
            'Attribute `on_cols` found of type {} but it can only be of `str`, `list` and `dict`'.format(
                type(cols)))

    return unpacking, expr


class FeatureOpsFactory:

    def __init__(self):
        pass

    @staticmethod
    def create(ops_type, data, on_cols, dimension):
        if ops_type == Metrics.COUNT:
            return CountFeatureOps(data=data, on_cols=on_cols, dimensions=dimension)
        elif ops_type == Metrics.MAX:
            return MaxFeatureOps(data=data, on_cols=on_cols, dimensions=dimension)
        elif ops_type == Metrics.SUM:
            return SumFeatureOps(data=data, on_cols=on_cols, dimensions=dimension)
        elif ops_type == Metrics.MIN:
            return MinFeatureOps(data=data, on_cols=on_cols, dimensions=dimension)
        elif ops_type == Metrics.COUNT_DISTINCT:
            return CountDistinctFeatureOps(data=data, on_cols=on_cols, dimensions=dimension)
        elif ops_type == Metrics.AVG:
            return AvgFeatureOps(data=data, on_cols=on_cols, dimensions=dimension)
        elif ops_type == Metrics.MEAN:
            return MeanFeatureOps(data=data, on_cols=on_cols, dimensions=dimension)
        elif ops_type == Metrics.SUM_DISTINCT:
            return SumDistinctFeatureOps(data=data, on_cols=on_cols, dimensions=dimension)
