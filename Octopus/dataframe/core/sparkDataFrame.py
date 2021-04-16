#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
import numpy as np
import pandas as pd

from Octopus.dataframe.core.frame import Frame, spark
from Octopus.dataframe.core.sparkSeries import Series
from Octopus.dataframe.core.utils import derived_from, exe_time, IdGenerator

from pyspark.sql.types import IntegerType, StringType, FloatType, DoubleType, LongType

__all__ = ["SparkDataFrame"]


class SparkDataFrame(Frame):
    """
    Distributed pandas-like SparkDataFrame

    Do not use this class directly.  Instead use functions like
    ``od.read_csv`` or ``od.from_pandas``.

    Parameters
    ----------
    sdf:
        The Spark DataFrame to format the Octopus SparkDataFrame.
    index_col:
        The column which acts as  the index to use for resulting frame.
    is_cached:
        Wether to cache the DataFrame in RAM.
    """

    def __init__(self, sdf=None, index_col=None, is_cache=False, pdf=None, df_id=None,
                 partition_info=None, use_secondary_index=False, use_local_index=False):
        """
        Octopus SparkDataFrame is a pandas-like interface focusing on big data processing under Spark.
        """
        Frame.__init__(self, sdf=sdf, index_col=index_col, is_cache=is_cache,
                       pdf=pdf, df_id=df_id,
                       partition_info=partition_info,
                       use_secondary_index=use_secondary_index,
                       use_local_index=use_local_index)
        self.name = ""

    def set_index(self, keys, drop=True, append=False, inplace=False, verify_integrity=False,
                  use_secondary_index=False, use_local_index=False):
        """
        Set the SparkDataFrame index (row labels) using one or more existing
        columns. By default yields a new object.

        Parameters
        ----------
        index :
            Which set the index of columns in the dataframe.

        Examples
        --------
        >>> df
           month  sale  year
        0  1      55    2012
        1  4      40    2014
        2  7      84    2013
        3  10     31    2014
        Set the index to become the 'month' column:
        >>> df.set_index('month')
               sale  year
        month
        1      55    2012
        4      40    2014
        7      84    2013
        10     31    2014
        """

        if self.use_pdf:
            if inplace:
                return self._pdata.set_index(keys, drop, append, inplace, verify_integrity)
            else:
                return SparkDataFrame(pdf=self._pdata.set_index(keys, drop, append, inplace, verify_integrity))

        if inplace:
            self.use_secondary_index = True
            if self.index_col is not None:
                self._delete_secondary_index()
                self._update(self.sdf.drop(self.index_col), keys, is_cache=False,
                             partition_info=self.partition_info, use_local_index=use_local_index,
                             use_secondary_index=use_secondary_index)
            self._update(self.sdf, keys, is_cache=False,
                         partition_info=self.partition_info, use_local_index=use_local_index,
                         use_secondary_index=use_secondary_index)
        else:
            if self.index_col is not None:
                self._delete_secondary_index()
                return SparkDataFrame(sdf=self.sdf.drop(self.index_col), index_col=keys, is_cache=False,
                                      partition_info=self.partition_info, use_local_index=use_local_index,
                                      use_secondary_index=use_secondary_index)
            return SparkDataFrame(sdf=self.sdf, index_col=keys, is_cache=False,
                                  partition_info=self.partition_info, use_local_index=use_local_index,
                                  use_secondary_index=use_secondary_index)

    def reset_index(self, drop=False, level=None, inplace=False, col_level=0, col_fill=''):
        """
        Reset the SparkDataFrame index with an extra serial index column.By default yields a new object.

        Parameters
        ----------
        drop :
            Whether to drop the original index of the dataframe.

        Examples
        --------
        >>> df
        class  max_speed
        falcon    bird      389.0
        parrot    bird       24.0
        lion    mammal       80.5
        monkey  mammal        NaN
        When we reset the index, the old index is added as a column, and a
        new sequential index is used:
        >>> df.reset_index()
            index   class  max_speed
        0  falcon    bird      389.0
        1  parrot    bird       24.0
        2    lion  mammal       80.5
        3  monkey  mammal        NaN
        We can use the `drop` parameter to avoid the old index being added as
        a column:
        >>> df.reset_index(drop=True)
            class  max_speed
        0    bird      389.0
        1    bird       24.0
        2  mammal       80.5
        3  mammal        NaN
        """
        if self.use_pdf:
            if inplace:
                return self._pdata.reset_index(drop=drop, level=level, inplace=inplace,
                                               col_level=col_level, col_fill=col_fill)
            return SparkDataFrame(pdf=self._pdata.reset_index(drop=drop, level=level, inplace=inplace,
                                                              col_level=col_level, col_fill=col_fill))
        sdf = self.sdf
        if self.index_col is not None and drop:
            sdf = sdf.drop(self.index_col)

        if inplace:
            if self.use_secondary_index:
                self._delete_secondary_index()
            return self._update(sdf, is_cache=False)

        return SparkDataFrame(sdf, is_cache=False)

    def get_column_type(self, name):
        """
        Get the type of some column.

        Parameters
        ----------
        name :
            The column name of the dataframe.
        """
        return dict(self.sdf.dtypes).get(name, 'string')

    def _get_ret_type(self, col):
        col_type = self.get_column_type(col)
        return {
            'int': IntegerType,
            'bigint': LongType,
            'double': DoubleType,
            'float': FloatType,
            'string': StringType
        }.get(col_type, StringType)

    def _ret_type(self, col, value):
        col_type = self.get_column_type(col)
        return {
            'int': int(value),
            'bigint': int(value),
            'double': float(value),
            'float': float(value),
            'string': str(value)
        }.get(col_type, str(value))

    @classmethod
    def from_pandas(cls, pdf, is_cache=True):
        sdf = spark.createDataFrame(pdf).repartition(200)
        sdf.show()
        return SparkDataFrame(sdf, is_cache=is_cache)

    def to_pandas(self):
        if self.pdf is None:
            self.pdf = self.sdf.toPandas()
        return self.pdf

    @classmethod
    def from_dask(cls, data):
        from Octopus.dataframe.core.daskDataFrame import DaskDataFrame
        if type(data) != DaskDataFrame:
            raise Exception("input data should be DaskDataFrame")
        pass

    def to_dask(self):
        # 小规模数据
        pdf = self.to_pandas()
        from Octopus.dataframe.core.daskDataFrame import DaskDataFrame
        return DaskDataFrame.from_pandas(pdf)

        # # 大规模数据
        # import uuid
        # from Octopus.dataframe.core.daskDataFrame import DaskDataFrame
        # path = "/shijun/test_data/tmp/" + uuid.uuid4() + "/df.csv"
        # self.to_csv(path)
        # return DaskDataFrame.from_csv(path)

    @classmethod
    def from_csv(cls, path, header=True, sep=',', index_col=0, parse_dates=True, encoding=None, tupleize_cols=False,
                 infer_datetime_format=False, is_cache=True):
        """Read CSV file.

        It is preferable to use the more powerful :func:`methods.read_csv`
        for most general purposes, but ``from_csv`` makes for an easy
        roundtrip to and from a file.

        Parameters
        ----------
        path : string file path or file handle / StringIO
        header : Boolean, default True
            Row to use as header (skip prior rows)
        sep : Not support.
        index_col : int or sequence, default 0
            Column to use for index.
        parse_dates : Not support.
        tupleize_cols : Not support.
        infer_datetime_format: Not support.
        is_cache: Whether to cache the dataframe in RAM.

        See also
        --------
        methods.read_csv

        Returns
        -------
        y : OctDataFrame
        """
        sdf = spark.read.csv(path=path, header=header,
                             inferSchema=False)  # , header=header, sep=sep, encoding=encoding)
        if isinstance(index_col, int):
            try:
                return SparkDataFrame(sdf, index_col=sdf.columns[index_col], is_cache=is_cache)
            except Exception as exp:
                raise Exception('beyond the original columns')
        elif isinstance(index_col, (list, tuple)):
            cols = []
            for col_num in index_col:
                cols.append(sdf.columns[col_num])
            try:
                pdf = sdf.toPandas().set_index(cols)
                return SparkDataFrame(spark.createDataFrame(pdf), is_cache=is_cache)
            except:
                raise Exception('beyond the original columns')
        return SparkDataFrame(sdf, is_cache=is_cache)

    def to_csv(self, path_or_buf=None, sep=",", na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='overwrite', encoding=None, compression=None, quoting=None, quotechar='"',
               line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        if self.use_pdf:
            self.sdf = spark.createDataFrame(data=self.pdf)

        print("write path:", path_or_buf)

        self.sdf.write.csv(path_or_buf, sep=sep, mode=mode, header=header)

    @classmethod
    def from_json(cls, path, is_cache=True):
        """Read JSON file.

        Parameters
        ----------
        path : string file path or file handle / StringIO
        is_cache: Whether to cache the dataframe in RAM.

        Returns
        -------
        y : OctDataFrame
        """
        sdf = spark.read.json(path)
        return SparkDataFrame(sdf, is_cache=is_cache)

    @classmethod
    def from_array(cls, data, cols=None, is_cache=True):
        """Read csv from array or list.

        Parameters
        ----------
        data : must be list or array
        is_cache: Whether to cache the dataframe in RAM.

        Returns
        -------
        y : OctDataFrame
        """
        if not isinstance(data, (list, np.ndarray)):
            raise TypeError("Type must be list or array.")
        l = data
        if isinstance(l, np.ndarray):
            l = l.tolist()
        if not isinstance(l[0], list):
            return Series.from_array(data=l, index=None)
        return SparkDataFrame(spark.createDataFrame(data=l, schema=cols), is_cache=is_cache)

    @classmethod
    def from_records(cls, data, index=None, exclude=None, columns=None, coerce_float=False, nrows=None, is_cache=True):
        """Read csv from records.

        Parameters
        ----------
        data : must be records

        Returns
        -------
        y : OctDataFrame
        """
        if not isinstance(data, np.recarray):
            raise BaseException(str(type(data)) + ' is not supported.')
        rec_list = data.tolist()
        sdf = spark.createDataFrame(rec_list, list(data.dtype.names))
        return SparkDataFrame(sdf, is_cache=True)

    @classmethod
    def from_dict(cls, data, orient='columns', dtype=None, is_cache=True):
        """Read csv from dict.

        Parameters
        ----------
        data : must be dict
        is_cache: Whether to cache the dataframe in RAM.
        Returns
        -------
        y : OctDataFrame
        """
        if not isinstance(data, dict):
            raise ValueError(str(type(data)) + ' is not supported.')
        values = list(data.values())
        keys = list(data.keys())
        if not isinstance(values[0], dict):
            values = list(map(lambda x: list(x), values))
            val_array = np.array(values)
            val_array = val_array.transpose()
            values = val_array.tolist()
            return SparkDataFrame(spark.createDataFrame(data=values, schema=keys), is_cache=is_cache)
        else:
            pass

    @classmethod
    def from_parquet(self, path, is_cache=True):
        """Read csv from parquet.

        Parameters
        ----------
        path : string file path or file handle / StringIO
        is_cache: Whether to cache the dataframe in RAM.

        Returns
        -------
        y : OctDataFrame
        """
        return SparkDataFrame(spark.read.parquet(path), is_cache=is_cache)

    def to_parquet(self, fname, engine='auto', compression='snappy', **kwargs):
        """
        Write a SparkDataFrame to the binary parquet format.

        Parameters
        ----------
        fname : str
            String file path.
        engine : Not support.
        compression : {'snappy', 'gzip', 'brotli', None}, default 'snappy'
            Name of the compression to use. Use ``None`` for no compression.
        """
        if self.use_pdf:
            self.sdf = spark.createDataFrame(self._pdata)

        self.sdf.write.parquet(path=fname, compression=compression)

    def to_dict(self, orient='dict', into=dict):
        """
        Convert the SparkDataFrame to a dictionary.
        The type of the key-value pairs can be customized with the parameters
        (see below).

        Parameters
        ----------
        orient : str {'dict', 'list', 'series', 'split', 'records', 'index'}
            Determines the type of the values of the dictionary.
            - 'dict' (default) : dict like {column -> {index -> value}}
            - 'list' : dict like {column -> [values]}
            - 'series' : dict like {column -> Series(values)}
            - 'split' : dict like
              {'index' -> [index], 'columns' -> [columns], 'data' -> [values]}
            - 'records' : list like
              [{column -> value}, ... , {column -> value}]
            - 'index' : dict like {index -> {column -> value}}
            Abbreviations are allowed. `s` indicates `series` and `sp`
            indicates `split`.
        into : class, default dict
            The collections.Mapping subclass used for all Mappings
            in the return value.  Can be the actual class or an empty
            instance of the mapping type you want.  If you want a
            collections.defaultdict, you must pass it initialized.

        Returns
        -------
        result : collections.Mapping like {column -> {index -> value}}
        """
        if not self.use_pdf:
            warnings.warn("Big Data just take limit data into dict to prevent oom.")
        return self._pdata.to_dict(orient=orient, into=into)

    def toJSON(self, use_unicode=True):
        """
        Converts a SparkDataFrame into a RDD of string.
        Each row is turned into a JSON document as one element in the returned RDD.

        Parameters
        ----------
        use_unicode : Whether to use unicode.
        """
        if self.use_pdf:
            self.sdf = spark.createDataFrame(self._pdata)

        return self.sdf.toJSON(use_unicode=use_unicode)

    def as_matrix(self, columns=None):
        """Convert the frame to its Numpy-array representation."""
        return self._pdata.as_matrix(columns)

    def to_json(self, path_or_buf=None, orient=None, date_format=None,
                double_precision=10, force_ascii=True, date_unit='ms',
                default_handler=None, lines=False, compression=None):
        """
        Saves the content of the SparkDataFrame in JSON format (JSON Lines text format or newline-delimited JSON) at the
        specified path.

        Parameters
        ----------
        path : the path in any Hadoop supported file system
        compression : compression codec to use when saving to file. This can be one of the known case-insensitive
            shorten names (none, bzip2, gzip, lz4, snappy and deflate).
        """
        if self.use_pdf:
            self.sdf = spark.createDataFrame(self._pdata)

        self.sdf.write.json(path=path_or_buf, compression=compression)

    def head(self, n):
        """
        Return the first `n` rows without re-ordering.

        Parameters
        ----------
        n : number of first rows
        """
        if self.use_pdf:
            return SparkDataFrame(pdf=self._pdata.head(n))

        row_count = self.record_nums
        col_count = len(self.columns)
        if row_count == 1:
            return self.iloc[:, :min(n, col_count)]
        else:
            return self.iloc[:min(n, row_count)]

    # Todo: 利用索引优化
    def tail(self, n):
        """
        Return the last `n` rows without re-ordering.

        Parameters
        ----------
        n : number of last rows
        """
        if self.use_pdf:
            return SparkDataFrame(pdf=self._pdata.tail(n))

        row_count = self.record_nums
        col_count = len(self.columns)
        if row_count == 1:
            return self.iloc[:, max(0, col_count - n):]
        else:
            return self.iloc[max(0, row_count - n)::, :]

    def describe(self, percentiles=None, include=None, exclude=None):
        """
        Generate descriptive statistics of SparkDataFrame columns.

        Parameters
        ----------
        percentiles : Not support.
        include : Not support.
        exclude : Not support.
        """
        if self.use_pdf:
            return SparkDataFrame(pdf=self._pdata.describe(percentiles=percentiles, include=include, exclude=exclude))

        cols = list(self.columns)
        sdf_new = self.sdf.describe(cols)
        return SparkDataFrame(sdf_new, is_cache=False).set_index("summary")

    def max(self, axis=0, skipna=True, level=None, numeric_only=None, **kwargs):
        """
        Return max of the dataframe

        Parameters
        ----------
        axis : Not support.
        skipna : Not support.
        split_every : Not support.
        """
        if self.use_pdf:
            return Series(pdf=self._pdata.max(axis=axis, skipna=skipna, level=level,
                                              numeric_only=numeric_only))

        if self.index_manager is None:
            d = {}
            cols = list(self.columns)
            for col in cols:
                d[col] = 'max'
            return Series(self.sdf.agg(d), is_cache=False)

        if self.index_manager.df_max is None:
            self.index_manager.get_aggregation()

        return self.index_manager.df_max

    def count(self, axis=0, skipna=True, level=None, numeric_only=None, **kwargs):
        """
        Count non-NA cells for each column or row.
        The values `None`, `NaN`, `NaT`, and optionally `numpy.inf` (depending
        on `pandas.options.mode.use_inf_as_na`) are considered NA.

        Parameters
        ----------
        axis : {0 or 'index', 1 or 'columns'}, default 0
            If 0 or 'index' counts are generated for each column.
            If 1 or 'columns' counts are generated for each **row**.
        level : Not support.
        numeric_only : Not support.

        Returns
        -------
        Series or SparkDataFrame
            For each column/row the number of non-NA/null entries.
            If `level` is specified returns a `SparkDataFrame`.

        See Also
        --------
        Series.count: number of non-NA elements in a Series
        SparkDataFrame.shape: number of SparkDataFrame rows and columns (including NA elements)
        SparkDataFrame.isna: boolean same-sized SparkDataFrame showing places of NA
        elements

        Examples
        --------
        >>> df
           Person   Age  Single
        0    John  24.0   False
        1    Myla   NaN    True
        2    None  21.0    True
        3    John  33.0    True
        4    Myla  26.0   False
        Notice the uncounted NA values:
        >>> df.count()
        Person    4
        Age       4
        Single    5
        dtype: int64
        Counts for each **row**:
        >>> df.count(axis='columns')
        0    3
        1    2
        2    2
        3    3
        4    3
        dtype: int64
        Counts for one level of a `MultiIndex`:
        >>> df.set_index(["Person", "Single"]).count(level="Person")
                Age
        Person
        John      2
        Myla      1
        """
        if self.use_pdf:
            return Series(pdf=self._pdata.count(axis=axis, skipna=skipna, level=level,
                                              numeric_only=numeric_only))

        if self.index_manager is None:
            d = {}
            cols = list(self.columns)
            for col in cols:
                d[col] = 'count'
            return Series(self.sdf.agg(d), is_cache=False)

        if self.index_manager.df_count is None:
            self.index_manager.get_aggregation()

        return self.index_manager.df_max

    def min(self, axis=0, skipna=True, level=None, numeric_only=None, **kwargs):
        """
        Return min of the dataframe

        Parameters
        ----------
        axis : Not support.
        skipna : Not support.
        split_every : Not support.
        """
        if self.use_pdf:
            return Series(pdf=self._pdata.min(axis=axis, skipna=skipna, level=level,
                                              numeric_only=numeric_only))

        if self.index_manager is None:
            d = {}
            cols = list(self.columns)
            for col in cols:
                d[col] = 'min'
            return Series(self.sdf.agg(d), is_cache=False)

        if self.index_manager.df_max is None:
            self.index_manager.get_aggregation()

        return self.index_manager.df_min

    def sum(self, axis=0, skipna=True, level=None, numeric_only=None, **kwargs):
        """
        Return sum of the dataframe

        Parameters
        ----------
        axis : Not support.
        skipna : Not support.
        """
        if self.use_pdf:
            return Series(pdf=self._pdata.sum(axis=axis, skipna=skipna, level=level,
                                              numeric_only=numeric_only), index_col=self.index_col)

        if self.index_manager is None:
            d = {}
            cols = list(self.columns)
            for col in cols:
                d[col] = 'sum'
            return Series(self.sdf.agg(d), is_cache=False)

        if self.index_manager.df_max is None:
            self.index_manager.get_aggregation()

        return self.index_manager.df_sum

    def mean(self, axis=0, skipna=True, level=None, numeric_only=None, **kwargs):
        """
        Return mean of the dataframe

        Parameters
        ----------
        axis : Not support.
        skipna : Not support.
        split_every : Not support.
        """
        if self.use_pdf:
            return Series(pdf=self._pdata.mean(axis=axis, skipna=skipna, level=level,
                                              numeric_only=numeric_only))
        d = {}
        cols = list(self.columns)
        for col in cols:
            d[col] = 'mean'
        return Series(self.sdf.agg(d), is_cache=False)

    def _is_bool_indexer(self, key):
        if isinstance(key, list):
            try:
                for x in key:
                    if type(key) != bool:
                        return False
                return len(key) == len(self.index)
            except TypeError:  # pragma: no cover
                return False
        return False

    def _getitem_multilevel(self, key):
        raise Exception("Not Support yet")

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        """
        Join columns with other SparkDataFrame either on index or on a key
        column. Efficiently Join multiple SparkDataFrame objects by index at once by
        passing a list.

        Parameters
        ----------
        other : SparkDataFrame, Series with name field set, or list of SparkDataFrame
            Index should be similar to one of the columns in this one. If a
            Series is passed, its name attribute must be set, and that will be
            used as the column name in the resulting joined SparkDataFrame
        on : name, tuple/list of names, or array-like
            Column or index level name(s) in the caller to join on the index
            in `other`, otherwise joins index-on-index. If multiple
            values given, the `other` SparkDataFrame must have a MultiIndex. Can
            pass an array as the join key if it is not already contained in
            the calling SparkDataFrame. Like an Excel VLOOKUP operation
        how : {'left', 'right', 'outer', 'inner'}, default: 'left'
            How to handle the operation of the two objects.
            * left: use calling frame's index (or column if on is specified)
            * right: use other frame's index
            * outer: form union of calling frame's index (or column if on is
              specified) with other frame's index, and sort it
              lexicographically
            * inner: form intersection of calling frame's index (or column if
              on is specified) with other frame's index, preserving the order
              of the calling's one
        lsuffix : string
            Suffix to use from left frame's overlapping columns
        rsuffix : string
            Suffix to use from right frame's overlapping columns
        sort : Not support.
        """
        if self.use_pdf and other.use_pdf:
            return SparkDataFrame(
                pdf=self._pdata.join(other._pdata, on=on, how=how, lsuffix=lsuffix, rsuffix=rsuffix, sort=sort))

        if self.index_col is None:
            self._add_id_column()
            # raise Exception("Not support.")

        left_index = True if on is None else False
        if on is not None and not isinstance(on, str):
            raise Exception("The caller's join key does not support MultiIndex.")
        return self.merge(right=other, left_index=left_index, right_index=True, left_on=on, how=how,
                          suffixes=(lsuffix, rsuffix))

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False,
              suffixes=('_x', '_y'), copy=True, indicator=False, validate=None):
        if self.use_pdf and right.use_pdf:
            return SparkDataFrame(
                pdf=self._pdata.merge(right._pdata, how=how, on=on, left_on=left_on, right_on=right_on,
                                      left_index=left_index,
                                      right_index=right_index, suffixes=suffixes, copy=copy, indicator=indicator,
                                      validate=validate))

        if self.use_pdf and self.sdf is None:
            self.sdf = spark.createDataFrame(self._data)

        if right.use_pdf and right.sdf is None:
            right.sdf = spark.createDataFrame(right._pdata)

        if self.index_col is None:
            if left_index or right_index:
                raise Exception("Not support")

        if not isinstance(right, SparkDataFrame):
            raise Exception("Only support parameter 'right' of SparkDataFrame !")
        drop_cols = []
        common_cols = list(set(self.columns) & set(right.columns))

        if not left_index and not right_index and self.index_col is not None:
            if isinstance(self.index_col, list):
                sA = self.sdf
                for col in self.index_col:
                    sA = sA.drop(col)
            else:
                sA = self.sdf.drop(self.index_col)
        else:
            sA = self.sdf

        if not right_index and not left_index and right.index_col is not None:
            if isinstance(right.index_col, list):
                sB = right.sdf
                for col in right.index_col:
                    sB = sB.drop(col)
            else:
                sB = right.sdf.drop(right.index_col)
        else:
            sB = right.sdf

        if right_index and left_index:
            if isinstance(self.index_col, str):
                left_on = [self.index_col]
            elif isinstance(self.index_col, list) and len(self.index_col) == 1:
                left_on = self.index_col
            else:
                raise Exception("index_col should be string or list")

            if isinstance(right.index_col, str):
                right_on = [right.index_col]
            elif isinstance(right.index_col, list) and len(right.index_col) == 1:
                right_on = right.index_col
            else:
                raise Exception("index_col should be string or list")

            on = None

        elif left_index:
            if isinstance(self.index_col, str):
                left_on = [self.index_col]
            elif isinstance(self.index_col, list) and len(self.index_col) == 1:
                left_on = self.index_col
            else:
                raise Exception("index_col should be string or list")

            if right.index_col == self.index_col:
                sB = sB.withColumnRenamed(right.index_col, right.index_col + "_right")
                right.index_col += "_right"
            if right_on is None:
                right_on = on
            on = None
        elif right_index:
            right_on = [right.index_col]
            if self.index_col == right.index_col:
                sA = sA.withColumnRenamed(self.index_col, self.index_col + "_left")
                self.index_col += "_left"
            if left_on is None:
                left_on = on
            on = None

        if on is None:
            if left_on is None and right_on is None:
                left_on = []
                right_on = []
                on = common_cols
                if len(on) == 0:
                    raise Exception("No key can be set as join key.")
            elif (left_on is None and right_on is not None) or (right_on is None and left_on is not None):
                raise Exception("Should set one of left_on key or index \
                                and one of right_on key or index at the same time.")
            else:
                if isinstance(left_on, str):
                    left_on = [left_on]
                if isinstance(right_on, str):
                    right_on = [right_on]

                if len(left_on) != len(right_on):
                    raise Exception("'left_on' and 'right_on' should have same length of keys.")
                else:
                    on = []
                    for key1, key2 in zip(left_on, right_on):
                        if key1 != key2:
                            sA = sA.withColumn(key1 + key2, sA[key1])
                            sB = sB.withColumn(key1 + key2, sB[key2])
                            on.append(key1 + key2)
                            drop_cols.append(key1 + key2)
                        else:
                            on.append(key1)
        else:
            if left_on is not None or right_on is not None:
                raise Exception("Parameter 'on' can not be assigned at the same \
                                time with parameters of  'left_on' and 'right_on' .")
            elif isinstance(on, str):
                on = [on]
            left_on = []
            right_on = []

        for col in sA.columns:
            if col in common_cols and col not in on and col not in left_on:
                sA = sA.withColumnRenamed(col, col + suffixes[0])

        for col in sB.columns:
            if col in common_cols and col not in on and col not in right_on:
                sB = sB.withColumnRenamed(col, col + suffixes[1])

        ret = sA.join(other=sB, on=on, how=how).sort(on)
        for col in drop_cols:
            ret = ret.drop(col)

        from pyspark.sql.types import StringType
        from pyspark.sql.functions import udf

        def combine(key1, key2):
            if key1 is None or key1 is np.NaN:
                return key2
            return key1

        combine_udf = udf(combine, StringType())
        if not left_index and right_index:
            replace = self.index.tolist()[-1]
        elif left_index and not right_index:
            replace = right.index.tolist()[-1]

        def reset1(value):
            if value is None or value is np.NaN:
                return replace
            return value

        reset_udf = udf(reset1, StringType())

        res_index = None
        accompany_index = None
        if left_index and right_index:
            if self.index_col == right.index_col:
                res_index = self.index_col
            else:
                if how == "right":
                    res_index = right.index_col
                    accompany_index = self.index_col
                else:
                    res_index = self.index_col
                    accompany_index = right.index_col

                ret = ret.withColumn(res_index, combine_udf(ret[res_index], ret[accompany_index]))
                ret = ret.drop(accompany_index)
                if how in ["inner", "outer"]:
                    ret = ret.withColumnRenamed(res_index, "index")
                    res_index = "index"

        elif left_index:
            res_index = right.index_col
            accompany_index = self.index_col
            ret = ret.withColumn(res_index, reset_udf(ret[res_index]))
            ret = ret.withColumn(right_on[0], combine_udf(ret[right_on[0]], ret[accompany_index]))
            ret = ret.drop(accompany_index)

        elif right_index:
            res_index = self.index_col
            accompany_index = right.index_col
            ret = ret.withColumn(res_index, reset_udf(ret[res_index]))
            ret = ret.withColumn(left_on[0], combine_udf(ret[left_on[0]], ret[accompany_index]))
            ret = ret.drop(accompany_index)

        if res_index is not None:
            ret = ret.sort(res_index)
            if res_index[-5::1] == "_left":
                ret = ret.withColumnRenamed(res_index, res_index[:-5:])
                res_index = res_index[:-5:]
            elif res_index[-6::1] == "_right":
                ret = ret.withColumnRenamed(res_index, res_index[:-6:])
                res_index = res_index[:-6:]

        return SparkDataFrame(ret, res_index, is_cache=False)

    # def __getattr__(self, name):
    #     print("name", name, self.columns)
    #     if name in self.columns:
    #         return self[name]
    #     return object.__getattr__(self, name)

    def groupbymin(self, groupby_obj):
        return self.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                            as_index=groupby_obj.as_index, sort=groupby_obj.sort, group_keys=groupby_obj.group_keys,
                            squeeze=groupby_obj.squeeze).min()

    def groupbymax(self, groupby_obj):
        return self.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                            as_index=groupby_obj.as_index, sort=groupby_obj.sort, group_keys=groupby_obj.group_keys,
                            squeeze=groupby_obj.squeeze).max()

    def groupbymean(self, groupby_obj):
        return self.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                            as_index=groupby_obj.as_index, sort=groupby_obj.sort, group_keys=groupby_obj.group_keys,
                            squeeze=groupby_obj.squeeze).mean()

    def groupbysum(self, groupby_obj):
        return self.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                            as_index=groupby_obj.as_index, sort=groupby_obj.sort, group_keys=groupby_obj.group_keys,
                            squeeze=groupby_obj.squeeze).sum()

    def groupbycount(self, groupby_obj):
        return self.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                            as_index=groupby_obj.as_index, sort=groupby_obj.sort, group_keys=groupby_obj.group_keys,
                            squeeze=groupby_obj.squeeze).count()

    def drop(self, labels=None, axis=0, index=None, columns=None, level=None,
             inplace=False, errors='raise'):
        """
        Drop specified labels from rows or columns.
        Remove rows or columns by specifying label names and corresponding
        axis, or by specifying directly index or column names. When using a
        multi-index, labels on different levels can be removed by specifying
        the level.

        Parameters
        ----------
        labels : single label or list-like
            Index or column labels to drop.
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Whether to drop labels from the index (0 or 'index') or
            columns (1 or 'columns').
        index, columns : single label or list-like
            Alternative to specifying axis (``labels, axis=1``
            is equivalent to ``columns=labels``).
            .. versionadded:: 0.21.0
        level : int or level name, optional
            For MultiIndex, level from which the labels will be removed.
        inplace : bool, default False
            If True, do operation inplace and return None.
        errors : {'ignore', 'raise'}, default 'raise'
            If 'ignore', suppress error and only existing labels are
            dropped.

        Returns
        -------
        dropped : DataFrame

        Examples
        --------
        >>> df
           A  B   C   D
        0  0  1   2   3
        1  4  5   6   7
        2  8  9  10  11
        Drop columns
        >>> df.drop(['B', 'C'], axis=1)
           A   D
        0  0   3
        1  4   7
        2  8  11
        >>> df.drop(columns=['B', 'C'])
           A   D
        0  0   3
        1  4   7
        2  8  11
        Drop a row by index
        >>> df.drop([0, 1])
           A  B   C   D
        2  8  9  10  11
        """
        if self.use_pdf:
            if inplace:
                return self._pdata.drop(labels=labels, axis=axis, index=index, columns=columns,
                                        level=level, inplace=inplace, errors=errors)
            else:
                return SparkDataFrame(pdf=self._pdata.drop(labels=labels, axis=axis,
                                                           index=index, columns=columns,
                                                           level=level, inplace=inplace,
                                                           errors=errors))

        if labels is not None:
            if index is not None or columns is not None:
                raise ValueError("Cannot specify both 'labels' and "
                                 "'index'/'columns'")
            axes = {axis: labels}
        elif index is not None or columns is not None:
            axes = {0: index, 1: columns}
        else:
            raise ValueError("Need to specify at least one of 'labels', "
                             "'index' or 'columns'")

        sdf = self.sdf
        for axis, labels in axes.items():
            if labels is not None:
                sdf = self._drop_axis(sdf, labels, axis, level=level, errors=errors)
        if inplace:
            return self._update(sdf)
        return SparkDataFrame(sdf, is_cache=False)

    def drop_duplicates(self, subset=None, keep='first', inplace=False):
        """
        Return SparkDataFrame with duplicate rows removed, optionally only
        considering certain columns

        Parameters
        ----------
        subset : column label or sequence of labels, optional
            Only consider certain columns for identifying duplicates, by
            default use all of the columns
        keep : Not support.
        inplace : Not support.

        Returns
        -------
        deduplicated : SparkDataFrame
        """
        if self.use_pdf:
            if inplace:
                self._pdata.drop_duplicates(subset=subset, keep=keep, inplace=inplace)
            return SparkDataFrame(
                    pdf=self._pdata.drop_duplicates(subset=subset,
                                                    keep=keep,
                                                    inplace=inplace)
            )

        if subset is None:
            subset = [x for x in self.columns if x != self.index_col]
        elif not isinstance(subset, list):
            subset = [subset]

        if self.index_col is not None:
            sdf = self.sdf.dropDuplicates(subset).sort(self.index_col)
        else:
            sdf = self.sdf.dropDuplicates(subset)

        if inplace:
            return self._update(sdf)
        return SparkDataFrame(sdf, is_cache=False)

    def _get_axis(self, axis):
        if axis == 0:
            return self.index
        elif axis == 1:
            return self.columns
        else:
            raise Exception("Only support 2 dimensions now.")

    @exe_time
    def filter(self, items=None, like=None, regex=None, axis=None):
        """
        Subset rows or columns of dataframe according to labels in
        the specified index.
        Note that this routine does not filter a dataframe on its
        contents. The filter is applied to the labels of the index.

        Parameters
        ----------
        items : list-like
            List of info axis to restrict to (must not all be present)
        like : string
            Keep info axis where "arg in col == True"
        regex : string (regular expression)
            Keep info axis with re.search(regex, col) == True
        axis : int or string axis name
            The axis to filter on.  By default this is the info axis,
            'index' for Series, 'columns' for SparkDataFrame

        Returns
        -------
        same type as input object

        Examples
        --------
        >>> df
        one  two  three
        mouse     1    2      3
        rabbit    4    5      6
        >>> # select columns by name
        >>> df.filter(items=['one', 'three'])
        one  three
        mouse     1      3
        rabbit    4      6
        >>> # select columns by regular expression
        >>> df.filter(regex='e$', axis=1)
        one  three
        mouse     1      3
        rabbit    4      6
        >>> # select rows containing 'bbi'
        >>> df.filter(like='bbi', axis=0)
        one  two  three
        rabbit    4    5      6
        """
        if self.use_pdf:
            return SparkDataFrame(pdf=self._pdata.filter(items=items, like=like, regex=regex, axis=axis))

        import re
        from pandas.core.common import count_not_none
        nkw = count_not_none(items, like, regex)
        if nkw > 1:
            raise TypeError('Keyword arguments `items`, `like`, or `regex` '
                            'are mutually exclusive')

        if axis is None:
            axis = 1

        if axis == 1:
            labels = self._get_axis(axis)

            if items is not None:
                values = [item for item in items if item in labels.tolist()]
            elif like:
                def f(x):
                    if not isinstance(x, str):
                        x = str(x)
                    return like in x

                likes = labels.map(f).tolist()
                values = []
                for i in range(len(likes)):
                    if likes[i]:
                        values.append(labels[i])
            elif regex:
                matcher = re.compile(regex)
                match = labels.map(lambda x: matcher.search(str(x)) is not None).tolist()
                values = []
                for i in range(len(match)):
                    if match[i]:
                        values.append(labels[i])
            else:
                raise TypeError('Must pass either `items`, `like`, or `regex`')

            return self.loc[:, values]

        else:
            if self.index_manager is None:
                index_col = self.index_col
                df_id = IdGenerator.next_id()

                def f(splitIndex, iterator):
                    def filter(origin_data):
                        if items is not None:
                            if origin_data[index_col] in items:
                                return origin_data
                        elif like:
                            if like in str(origin_data[index_col]):
                                return origin_data
                        elif regex:
                            matcher = re.compile(regex)
                            if matcher.search(str(origin_data[index_col])) is not None:
                                return origin_data

                    res = []
                    try:
                        while iterator:
                            record = next(iterator)
                            ret = filter(record)
                            if ret is not None:
                                res.append(ret)
                    except Exception:
                        pass

                    # filter global index build optimization
                    # try:
                    #    from redis import Redis
                    #    r = Redis(host='simple27', port=6379, decode_responses=True)
                    #    r.hmset("df" + str(df_id), {str(splitIndex): len(res)})
                    # except Exception:
                    #    print("write meta_data of dataframe {} to redis failed" % new_df_id)
                    return res

                rdd = self.sdf.rdd.mapPartitionsWithIndex(f)
                if rdd.count() == 0:
                    from Octopus.dataframe.core.sparkinit import spark
                    sdf = spark.createDataFrame([], self.sdf.schema)
                else:
                    sdf = rdd.toDF()

                return SparkDataFrame(sdf=sdf, index_col=index_col, df_id=df_id)

            else:
                def f3(split_index, iter):

                    import time
                    t0 = time.time()

                    object_id = next(iter)

                    import pyarrow.plasma as plasma
                    import pyarrow as pa
                    client = plasma.connect("/tmp/plasma", "", 0)

                    if not client.contains(object_id):
                        raise Exception("The node does not contain object_id of ", str(object_id))

                    # Fetch the Plasma object
                    [data] = client.get_buffers([object_id])  # Get PlasmaBuffer from ObjectID
                    buffer = pa.BufferReader(data)

                    # Convert object back into an Arrow RecordBatch
                    reader = pa.RecordBatchStreamReader(buffer)
                    record_batch = reader.read_next_batch()

                    # Convert back into Pandas
                    pdf = record_batch.to_pandas()
                    res = pdf.filter(items=items, like=like, regex=regex, axis=axis)

                    from pyspark.sql.types import Row

                    def to_row(row_data):
                        row_data = row_data.to_dict()
                        row = Row(*row_data)
                        row = row(*row_data.values())
                        return row

                    res = list(res.apply(to_row, axis=1))

                    return res

                from Octopus.dataframe.core.sparkinit import spark
                return self.index_manager.index_rdd.mapPartitionsWithIndex(f3).toDF()

    def sort_values(self, by=None, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        """
        Sort object by labels (along an axis)

        Parameters
        ----------
        by : Columns by which to sort the dataframe.
        axis : Not support.
        ascending : boolean, default True
            Sort ascending vs. descending
        inplace : bool, default False
            if True, perform operation in-place
        kind : Not support.
        na_position : Not support.
        """
        if self.use_pdf:
            pdf = self._pdata.sort_values(by=by, axis=axis, ascending=ascending,
                                          inplace=inplace, kind=kind, na_position=na_position)
            if inplace:
                return None
            return SparkDataFrame(pdf=pdf)

        if isinstance(by, str):
            by = [by]
        if isinstance(ascending, bool):
            ascending = [ascending] * len(by)
        if inplace:
            self._update(self.sdf.orderBy(by, ascending=ascending), self.index_col)
        else:
            return SparkDataFrame(self.sdf.orderBy(by, ascending=ascending), self.index_col)

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False, **kwargs):
        """
        Group series using mapper (dict or key function, apply given function
        to group, return result as series) or by a series of columns.

        Parameters
        ----------
        by : mapping, function, label, or list of labels
            Used to determine the groups for the groupby.
            If ``by`` is a function, it's called on each value of the object's
            index. If a dict or Series is passed, the Series or dict VALUES
            will be used to determine the groups (the Series' values are first
            aligned; see ``.align()`` method). If an ndarray is passed, the
            values are used as-is determine the groups. A label or list of
            labels may be passed to group by the columns in ``self``. Notice
            that a tuple is interpreted a (single) key.
        axis : Not support.
        level : Not support.
        as_index : boolean, default True
            For aggregated output, return object with group labels as the
            index. Only relevant for SparkDataFrame input. as_index=False is
            effectively "SQL-style" grouped output
        sort : boolean, default True
            Sort group keys. Get better performance by turning this off.
            Note this does not influence the order of observations within each
            group.  groupby preserves the order of rows within each group.
        group_keys : Not support.
        squeeze : Not support.

        Returns
        -------
        GroupBy object
        """
        from Octopus.dataframe.core.groupby import groupby
        return groupby(self, by, as_index, sort)

    def to_spark(self):
        """Convert the Octopus SparkDataFrame to Spark SparkDataFrame"""
        if self.use_pdf:
            self.sdf = spark.createDataFrame(self._pdata)
        return self.sdf


def read_csv_spark(file_path, is_cache=False, repartition=False, repartition_num=72):
    """
    Read CSV (comma-separated) file into SparkDataFrame.
    """
    if repartition:
        sdf = spark.read.csv(file_path, header=True, inferSchema=True).repartition(repartition_num)
    else:
        sdf = spark.read.csv(file_path, header=True, inferSchema=True)

    return SparkDataFrame(sdf, is_cache=is_cache)
