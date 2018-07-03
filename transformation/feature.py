from transformation import ops
from transformation import feature_col as fc


class Feature(object):
    """
    Feature class represents the base class for the feature transformation.
    """

    def __init__(self, name, data_table=None, metric=None, on=None, dimensions=None):
        """
        Constructor of the `Feature` class!
        :param name: The name of the feature to develop with metric defined.
        :param data_table: The spark data frames which is the data table for feature computations.
        :param metric: This represents the metrics to be calculated like: `COUNT`, `MIN`, `AVG` etc.
        :param on: The column on which feature computation will be applied.
        :param dimensions: The aggregations granularity level.
        """
        self.name = name
        self.data_frame = data_table
        self.metric = metric
        self.on = on
        self.dimensions = dimensions

    def __repr__(self):
        return 'Feature({}, {}, {}, {})'.format(self.name, self.metric, self.on, self.dimensions)

    def transform(self, func):
        """
        Any intermediate transformation needs to be done!
        :param func:
        :return:
        """
        pass

    def build(self):
        """
        Factory ops will compute and return results!
        :return: FeatureColumn
        """
        result = ops.FeatureOpsFactory.create(self.metric, self.data_frame, self.on, self.dimensions).compute()
        return fc.FeatureColumn(col_name=self.name, col_value=result)

# class FeatureCombine(Feature):
#     """
#     TODO This class will be used to built master table!
#     """
#     pass
