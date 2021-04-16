#!/usr/bin/env python
# -*- coding: utf-8 -*-

from Octopus.dataframe.core.sparkDataFrame import read_csv_spark
from Octopus.dataframe.core.pandasDataFrame import read_csv_pandas
from Octopus.dataframe.core.daskDataFrame import read_csv_dask

__all__ = ["OctDataFrame"]

type_order = ("Spark", "Dask", "Pandas")


class OctDataFrame(object):
    def __init__(self, dataframe, engine_type="Pandas"):
        self.dataframe = dataframe
        self.engine_type = engine_type

    @classmethod
    def from_csv(cls, file_path, engine_type="Pandas"):
        if engine_type == "Pandas":
            dataframe = read_csv_pandas(file_path)
        elif engine_type == "Spark":
            dataframe = read_csv_spark(file_path)
        elif engine_type == "Dask":
            dataframe = read_csv_dask(file_path)
        else:
            raise ValueError("Not implement this method, read_dataframe, ")

        return OctDataFrame(dataframe, engine_type)

    @property
    def dtypes(self):
        return self.dataframe.dtypes

    @property
    def values(self):
        return self.dataframe.values

    @property
    def size(self):
        return self.dataframe.size()

    @property
    def shape(self):
        return self.dataframe.shape()

    def count(self, axis=0):
        return self.dataframe.count(axis=axis)

    def astype(self, dtype, copy=True, errors='raise', **kwargs):
        return OctDataFrame(self.dataframe.astype(dtype=dtype, copy=copy, errors=errors, **kwargs),
                            engine_type=self.engine_type)

    def __getitem__(self, key):
        return self.dataframe[key]

    def __setitem__(self, key, value):
        self.dataframe[key] = value

    @property
    def loc(self):
        return self.dataframe.loc(self)

    @property
    def iloc(self):
        return self.dataframe.iloc

    def set_index(self, keys, drop=True, append=False, inplace=False, verify_integrity=False,
                  use_secondary_index=False, use_local_index=False):
        if self.engine_type == "Spark":
            return self.dataframe.set_index(keys, drop, append, inplace, verify_integrity,
                                            use_secondary_index, use_local_index)
        else:
            return self.dataframe.set_index(keys, drop, append, inplace, verify_integrity)

    def drop(self, labels=None, axis=0, index=None, columns=None, level=None,
             inplace=False, errors='raise'):
        return self.dataframe.drop(labels=labels, axis=axis, index=index, columns=columns, level=level,
                                   inplace=inplace, errors=errors)

    def from_records(self, data, index=None, exclude=None, columns=None, coerce_float=False, nrows=None, is_cache=True):
        pass

    def from_json(self, path, is_cache=True):
        pass

    def from_pandas(self, pdf, is_cache=True):
        pass

    @classmethod
    def from_array(cls, data, cols=None, is_cache=True):
        pass

    def from_dict(cls, data, orient='columns', dtype=None, is_cache=True):
        pass

    def to_csv(self, path_or_buf=None, sep=",", na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='w', encoding=None, compression=None, quoting=None, quotechar='"',
               line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        return self.dataframe.to_csv(path_or_buf=path_or_buf, sep=sep, na_rep=na_rep, float_format=float_format,
                                     columns=columns, header=header, index=index,
                                     index_label=None, mode='w', encoding=None, compression=None, quoting=None,
                                     quotechar='"',
                                     line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None,
                                     doublequote=True,
                                     escapechar=None, decimal='.')

    def to_json(self, path_or_buf=None, orient=None, date_format=None,
                double_precision=10, force_ascii=True, date_unit='ms',
                default_handler=None, lines=False, compression=None):
        pass

    def to_records(self, index=True, convert_datetime64=True):
        pass

    def to_parquet(self, fname, engine='auto', compression='snappy', **kwargs):
        pass

    def from_parquet(self, path):
        pass

    def to_text(self, path, compression=None):
        pass

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False, **kwargs):
        return self.dataframe.groupby(by=by, axis=axis, level=level, as_index=as_index, sort=sort,
                                      group_keys=group_keys, squeeze=squeeze, **kwargs)

    def sort_values(self, by=None, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        return self.dataframe.sort_values(by=by, axis=axis, ascending=ascending, inplace=inplace,
                                          kind='quicksort', na_position='last')

    def filter(self, items=None, like=None, regex=None, axis=None):
        return self.dataframe.filter(items=items, like=like, regex=regex, axis=axis)

    def drop_duplicates(self, subset=None, keep='first', inplace=False):
        return self.dataframe.drop_duplicates(subset=subset, keep=keep, inplace=inplace)

    def drop(self, labels=None, axis=0, index=None, columns=None, level=None,
             inplace=False, errors='raise'):
        return self.dataframe.drop(labels=labels, axis=axis, index=index, columns=columns, level=level,
                                   inplace=inplace, errors=errors)

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False,
              suffixes=('_x', '_y'), copy=True, indicator=False, validate=None):
        return self.dataframe.merge(right, how=how, on=on, left_on=left_on, right_on=right_on, left_index=left_index,
                                    right_index=right_index,
                                    suffixes=suffixes,
                                    copy=copy,
                                    indicator=indicator,
                                    validate=validate)

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        return self.dataframe.join(other, on=on, how=how, lsuffix=lsuffix, rsuffix=rsuffix,
                                   sort=sort)

    def mean(self, axis=None, skipna=True, split_every=False):
        return self.dataframe.mean(axis=axis, skipna=skipna, split_every=split_every)

    def sum(self, axis=0, skipna=True, split_every=False):
        return self.dataframe.sum(axis=axis, skipna=skipna, split_every=split_every)

    def min(self, axis=0, skipna=True, split_every=False):
        return self.dataframe.min(axis=axis, skipna=skipna, split_every=split_every)

    def max(self, axis=0, skipna=True, split_every=False):
        return OctDataFrame(self.dataframe.max(axis=axis, skipna=skipna, split_every=split_every))

    def describe(self, percentiles=None, include=None, exclude=None):
        return self.dataframe.describe(percentiles=percentiles, include=include, exclude=exclude)

    def tail(self, n):
        return OctDataFrame(self.dataframe.tail(n), self.engine_type)

    def head(self, n):
        return OctDataFrame(self.dataframe.head(n), self.engine_type)

    def __str__(self):
        return self.dataframe.__str__()

    def __iter__(self):
        return self.dataframe.__iter__()


def read_csv(file_path, engine_type="Pandas"):
    result = None
    if engine_type == "Pandas":
        result = read_csv_pandas(file_path)
    elif engine_type == "Spark":
        result = read_csv_spark(file_path)
    elif engine_type == "Dask":
        result = read_csv_dask(file_path)
    else:
        raise ValueError("Not implement this method, read_dataframe, ")

    return OctDataFrame(result, engine_type)

# def as_octdataframe(octdataframe, engine_type):
#     """
#     transfrom octdataframe to engine_type
#     :param octdataframe: octdataframe
#     :param engine_type: engine type
#     :return: octdataframe
#     """
#     if engine_type == octdataframe.flag:
#         return octdataframe
#     location = octdataframe.dataframe.location
#     if is_in_memory(location):
#         name = build_dataframe_name()
#         path = build_dataframe_path(name)
#         octdataframe.write_dataframe(path, name)
#         octdataframe.dataframe.location = path
#         location = path
#     return read_oct_dataframe(location, engine_type)





