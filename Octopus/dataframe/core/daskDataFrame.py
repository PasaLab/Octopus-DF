#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client

from Octopus.dataframe.core.pandasDataFrame import PandasDataFrame
from Octopus.dataframe.core.abstractDataFrame import AbstractDataFrame

# client = Client(address="simple34:8786", n_workers=4, threads_per_worker=24, processes=True, memory_limit="25GB")


class DaskDataFrame(AbstractDataFrame):
    def __init__(self, data=None, index=None, columns=None, dtype=None,
                 copy=False):
        if type(data) == PandasDataFrame:
            self.dataframe = dd.from_pandas(data, npartitions=16)
        elif type(data) == dd.DataFrame or type(data) == dd.Series:
            self.dataframe = data
        else:
            self.dataframe = dd.from_pandas(pd.DataFrame(data=data, index=index, columns=columns, dtype=dtype,
                                                         copy=copy), npartitions=16)
        self.partitions = 16

    def __str__(self):
        self.dataframe.__str__()

    def filter(self, items=None, like=None, regex=None, axis=None):
        raise Exception("Dask DataFrame not support filter now")

    def head(self, n):
        return DaskDataFrame(self.dataframe.head(n))

    @property
    def size(self):
        return self.dataframe.size()

    def from_parquet(self, path):
        self.dataframe.from_parquet(path)

    def to_csv(self, path_or_buf=None, sep=",", na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='w', encoding=None, compression=None, quoting=None, quotechar='"',
               line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        self.dataframe.to_csv(path_or_buf, sep, na_rep, float_format, columns, header, index, index_label, mode,
                              encoding,
                              compression, quoting, quotechar, line_terminator, chunksize, tupleize_cols, date_format,
                              doublequote, escapechar, decimal)

    def to_json(self, path_or_buf=None, orient=None, date_format=None, double_precision=10, force_ascii=True,
                date_unit='ms', default_handler=None, lines=False, compression=None):
        self.dataframe.to_json(path_or_buf, orient, date_format, double_precision, force_ascii, date_unit,
                               default_handler,
                               lines, compression)

    @property
    def shape(self):
        return self.dataframe.shape()

    @property
    def values(self):
        return self.dataframe.values()

    @classmethod
    def from_pandas(cls, data):
        return DaskDataFrame(dd.from_pandas(data, npartitions=16))

    def to_pandas(self):
        from Octopus.dataframe.core.pandasDataFrame import PandasDataFrame
        return PandasDataFrame(self.dataframe.compute())

    @classmethod
    def from_spark(cls, sdf):
        pass

    def to_spark(self):
        # 小规模数据
        pdf = self.to_pandas()
        from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
        return SparkDataFrame.from_pandas(pdf)

        # # 大规模数据
        # import uuid
        # from Octopus.dataframe.core.sparkDataframe import SparkDataFrame
        # path = "/shijun/test_data/tmp/" + uuid.uuid4() + "/df.csv"
        # self.to_csv(path)
        # return SparkDataFrame.from_csv(path)

    def to_csv(self, path_or_buf=None, sep=",", na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='w', encoding=None, compression=None, quoting=None, quotechar='"',
               line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        self.dataframe.to_csv(path_or_buf)

    def to_parquet(self, fname, engine='auto', compression='snappy', **kwargs):
        self.dataframe.to_parquet(fname, engine, compression, **kwargs)

    def to_records(self, index=True, convert_datetime64=True):
        self.dataframe.to_records(index, convert_datetime64)

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True, group_keys=True, squeeze=False, **kwargs):
        self.dataframe.groupby(by, axis, level, as_index, sort, group_keys, squeeze, **kwargs)

    def drop_duplicates(self, subset=None, keep='first', inplace=False):
        self.dataframe.drop_duplicates(subset, keep, inplace)

    def from_dict(self, data, orient='columns', dtype=None, is_cache=True):
        self.dataframe.from_dict(data, orient, dtype, is_cache)

    def to_text(self, path, compression=None):
        self.dataframe.to_text(path, compression)

    def set_index(self, keys, drop=True, append=False, inplace=False, verify_integrity=False):
        return DaskDataFrame(self.dataframe.set_index(other=keys, drop=drop))

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='', sort=False):
        return DaskDataFrame(self.dataframe.join(other.dataframe, on, how, lsuffix, rsuffix))

    def __str__(self):
        return self.dataframe.__str__()

    def astype(self, dtype, copy=True, errors='raise', **kwargs):
        self.dataframe.astype(dtype, copy, errors, **kwargs)

    @property
    def dtypes(self):
        return self.dataframe.dtypes()

    @classmethod
    def from_csv(cls, path, header=True, sep=',', index_col=0, parse_dates=True, encoding=None, tupleize_cols=False,
                 infer_datetime_format=False, is_cache=True):
        return DaskDataFrame(
            dd.read_csv(path, header, sep, index_col, parse_dates, encoding, tupleize_cols, infer_datetime_format))

    def __setitem__(self, key, value):
        self.dataframe.__setitem__(key, value)

    def describe(self, percentiles=None, include=None, exclude=None):
        self.dataframe.describe(percentiles, include, exclude)

    def from_records(self, data, index=None, exclude=None, columns=None, coerce_float=False, nrows=None, is_cache=True):
        self.dataframe.from_records(data, index, exclude, columns, coerce_float, nrows, is_cache)

    def sort_values(self, by=None, axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last'):
        raise Exception("Dask not support function of sort_values")

    @property
    def iloc(self):
        return self.dataframe.iloc

    @property
    def loc(self):
        return self.dataframe.loc

    def __getitem__(self, key):
        return DaskDataFrame(self.dataframe[key])

    @classmethod
    def from_array(self, data, cols=None, is_cache=True):
        return self.dataframe.from_array(data, cols, is_cache)

    def from_json(self, path, is_cache=True):
        return self.dataframe.from_json(path, is_cache)

    def mean(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        return self.dataframe.mean(axis, skipna)

    def max(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        return self.dataframe.max(axis, skipna)

    def min(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        return self.dataframe.min(axis, skipna)

    def sum(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        return self.dataframe.sum(axis, skipna)

    def count(self, axis=None, level=None, numeric_only=None, **kwargs):
        return self.dataframe.count(axis)

    def tail(self, n):
        return DaskDataFrame(self.dataframe.tail(n))

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False,
              suffixes=('_x', '_y'), copy=True, indicator=False, validate=None):
        return self.dataframe.merge(right=right.dataframe, how=how, on=on, left_on=left_on, right_on=right_on,
                                    left_index=left_index, right_index=right_index, suffixes=suffixes)

    def drop(self, labels=None, axis=1, index=None, columns=None, level=None, inplace=False, errors='raise'):
        import warnings
        warnings.warn("Dask drop does not support index, columns, level, inplace parameters now.")
        return DaskDataFrame(self.dataframe.drop(labels, axis, errors))

    def groupbymin(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by).min()

    def groupbymax(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by).max()

    def groupbymean(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by).mean()

    def groupbysum(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by).sum()

    def groupbycount(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by).count()

    def drop_duplicates(self, subset=None, keep='first', inplace=False):
        logging.warning("Dask doesn't support the following argument(s).\n"
              "* subset\n"
              "* keep\n"
              "* inplace\n")
        return self.dataframe.drop_duplicates()


def read_csv_dask(filepath_or_buffer,
                  sep=',',
                  delimiter=None,
                  header='infer',
                  names=None,
                  index_col=None,
                  usecols=None,
                  squeeze=False, ):
    pdf = pd.read_csv(filepath_or_buffer, sep, delimiter, header, names, index_col, usecols, squeeze)
    ddf = dd.from_pandas(pdf, npartitions=16)
    return DaskDataFrame(data=ddf)
