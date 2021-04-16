#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from pandas.core.dtypes.common import is_integer

from Octopus.dataframe.core.frame import spark
from Octopus.dataframe.core.utils import exe_time
from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
from Octopus.dataframe.core.sparkSeries import Series

""" General functions """


__all__ = ["new_dd_object", "from_spark", "from_pandas",  "read_json", "read_csv", "getitem_axis",
           "get_df_or_series", "is_single", "is_empty_slice"]


def new_dd_object(sdf):
    """Generic constructor for octopus.dataframe objects.

    Decides the appropriate output class based on the type of `meta` provided.
    """
    return SparkDataFrame(sdf)


@exe_time
def from_spark(sdf):
    """Octopus SparkDataFrame is a pandas-like tool focusing on big data processing.

    This function will receive a Spark SparkDataFrame and return an Octopus SparkDataFrame.
    """
    return SparkDataFrame(sdf)


@exe_time
def from_pandas(data):
    """Octopus SparkDataFrame is a pandas-like tool focusing on big data processing.

    This function will receive a Pandas SparkDataFrame or Series and return an Octopus SparkDataFrame.
    """
    if not isinstance(data, (pd.Series, pd.DataFrame)):
        raise TypeError("Input must be a pandas SparkDataFrame or Series")
    return SparkDataFrame(pdf=data)


@exe_time
def read_json(path_or_buf=None, lines=False):
    """Octopus SparkDataFrame is a pandas-like tool focusing on big data processing.

    This function will read from json file and return Octopus SparkDataFrame.
    """

    return SparkDataFrame(spark.read.json(path_or_buf, multiLine=lines))


@exe_time
def read_csv(file_path, is_cache=False):
    """
    Read CSV (comma-separated) file into SparkDataFrame.
    """
    sdf = spark.read.csv(file_path, header=True, inferSchema=True).repartition(72)
    return SparkDataFrame(sdf=sdf, is_cache=is_cache)


read_csv_spark = read_csv


@exe_time
def repartititon(sdf, number, *cols):
    """
    Repartition spark SparkDataFrame, Spark Api can be useful reference.
    """
    return sdf.repartition(number, *cols)


@exe_time
def read_parquet(path, engine='auto', **kwargs):
    """
    Load a parquet object from the file path, returning a SparkDataFrame.

    Parameters
    ----------
    path : string
        File path
    columns: list, default=None
        If not None, only these columns will be read from the file.
    engine : {'auto', 'pyarrow', 'fastparquet'}, default 'auto'
        Parquet library to use. If 'auto', then the option
        ``io.parquet.engine`` is used. The default ``io.parquet.engine``
        behavior is to try 'pyarrow', falling back to 'fastparquet' if
        'pyarrow' is unavailable.
    kwargs are passed to the engine

    Returns
    -------
    SparkDataFrame
    """
    return SparkDataFrame(spark.read.parquet(path))


@exe_time
def is_single(obj):
    if isinstance(obj, (int, float, str)):
        return True
    return False


@exe_time
def getitem_axis(sdf, indexer, axis=0, index_col="index"):
    if isinstance(indexer, np.ndarray):
        indexer = indexer.tolist()

    if axis == 0:
        return sdf.where(sdf[index_col].isin(indexer))

    elif axis == 1:
        if index_col is not None:
            indexer = [index_col] + indexer
        return sdf.select(indexer)
    else:
        raise KeyError("Octopus only supports 2 dimensions now.")


@exe_time
def loc_element(sdf, indexer, axis=0, index_col="index"):
    # Todo: can use cache to optimize
    if axis == 0:
        return sdf.filter(sdf[index_col] == indexer)
    elif axis == 1:
        return sdf.select(index_col, indexer)
    else:
        raise KeyError("Octopus only supports 2 dimensions now.")


@exe_time
def to_pandas(sdf, index_col="index"):
    data = sdf.toPandas()
    if index_col is not None:
        data = data.set_index(index_col)
    return data


@exe_time
def check_single_axis(indexer):
    return is_single(indexer)


@exe_time
def get_df_or_series(sdf, iindexer, cindexer, index_col=None, is_df=True, name=None, is_single_row=False,
                     is_single_col=False, pdf=None, df_id=None, new_df_partition_info=None):
    if is_df:
        if is_single_row and is_single_col:
            if is_single(iindexer) and is_single(cindexer):
                return to_pandas(sdf, index_col).iloc[0, 0]
            elif is_single(iindexer):
                return Series(sdf, index_col=index_col, name=sdf.select(index_col).toPandas()[index_col].iloc[0],
                        is_cache=False, df_id=df_id, partition_info=new_df_partition_info)
            elif is_single(cindexer):
                return Series(sdf, index_col=index_col, is_cache=False, df_id=df_id,
                              partition_info=new_df_partition_info)
            else:
                return SparkDataFrame(sdf, None, is_cache=False, df_id=df_id,
                                      partition_info=new_df_partition_info)
        elif is_single_row and check_single_axis(iindexer):
            return Series(sdf, index_col=index_col, name=sdf.select(index_col).toPandas()[index_col].iloc[0],
                 is_cache=False, df_id=df_id, partition_info=new_df_partition_info)
        elif is_single_col and check_single_axis(cindexer):
            return Series(sdf, index_col=index_col, is_cache=False, df_id=df_id,
                          partition_info=new_df_partition_info)
        else:
            return SparkDataFrame(sdf, None, is_cache=False, pdf=pdf, df_id=df_id,
                                  partition_info=new_df_partition_info)
    else:
        if is_single_row and is_single_col and (is_single(iindexer) or is_single(cindexer)):
                return to_pandas(sdf, index_col).iloc[0, 0]
        return Series(sdf, index_col=index_col, name=name, is_cache=False, df_id=df_id,
                      partition_info=new_df_partition_info)


@exe_time
def is_empty_slice(indexer):
    if isinstance(indexer, slice):
        return indexer.start is None and indexer.stop is None and indexer.step is None
    return False


@exe_time
def is_integer_array(keys):
    if isinstance(keys, list):
        for key in keys:
            if not isinstance(key, int):
                return False
        return True
    return is_integer(keys)


