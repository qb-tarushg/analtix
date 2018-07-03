from enum import Enum


class Metrics(Enum):
    """
    ENUMS to represent feature transformations like 'COUNT', 'MAX' etc.
    """
    COUNT = 'count'
    AVG = 'avg'
    MIN = 'min'
    MAX = 'max'
    SUM = 'sum'
    COUNT_DISTINCT = 'countDistinct'
    SUM_DISTINCT = 'sumDistinct'
    MEAN = 'mean'
