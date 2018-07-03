class FeatureColumn:
    """
    This class acts as the result computed from the feature transformation.
    """

    def __init__(self, col_name, col_value):
        """
        Constructor of the class FeatureColumn.
        :param col_name: String name of the feature.
        :param col_value: Spark data frame as value stored.
        """
        self.col_name = col_name
        self.col_value = col_value

    @property
    def value(self):
        """
        Method to return spark dataframe.
        :return: DataFrame
        """
        return self.col_value

    @property
    def name(self):
        """
        Method to return name of the feature.
        :return: str
        """
        return self.col_name

    def __repr__(self):
        return 'Feature Column with (name={}, value={})'.format(self.col_name, type(self.col_value))
