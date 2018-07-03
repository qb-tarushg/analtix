def load(spark, path, file_format='parquet', options=None):
    """
    This method is used for loading/reading files from specified location.
    :param spark: spark session
    :param path: Location of the files.
    :param file_format: Format of file eg. csv, json, parquet
    :param options: this includes dictionary of different options like schema, inferSchema, headers
    :return: Dataframe
    """
    if path is None or path == '':
        raise ValueError('Path is not specified or it does not exists!')
    if options is not None:
        return spark.read.options(options).format(file_format).load(path)
    else:
        return spark.read.format(file_format).load(path)
