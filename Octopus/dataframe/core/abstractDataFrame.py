#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from Octopus.dataframe.core.utils import derived_from

__all__ = ["AbstractDataFrame"]


class AbstractDataFrame(object):
    def __init__(self,  data=None, index=None, columns=None, dtype=None,
                 copy=False):
        self.dataframe = data

    def __str__(self):
        return self.dataframe.__str__()

    def __getitem__(self, key):
        return AbstractDataFrame(self.dataframe[key])

    def __setitem__(self, key, value):
        self.dataframe[key] = value

    @property
    @derived_from(pd.DataFrame)
    def dtypes(self):
        return self._pdata.dtypes

    @property
    @derived_from(pd.DataFrame)
    def values(self):
        return self._pdata.values

    @property
    @derived_from(pd.DataFrame)
    def size(self):
        if self.use_pdf:
            return self._pdata.size
        return len(self.columns) * self.count()

    @property
    @derived_from(pd.DataFrame)
    def shape(self):
        if self.use_pdf:
            return self._pdata.shape

        return self.record_nums, len(self.columns)

    def astype(self, dtype, copy=True, errors='raise', **kwargs):
        pass

    @property
    @derived_from(pd.DataFrame)
    def loc(self):
        return self.dataframe.loc

    @property
    @derived_from(pd.DataFrame)
    def iloc(self):
        return self.dataframe.iloc

    def set_index(self, keys, drop=True, append=False, inplace=False, verify_integrity=False):
        return AbstractDataFrame(self.dataframe.set_index(keys=keys,
                                 drop=drop,
                                 append=append,
                                 inplace=inplace,
                                 verify_integrity=verify_integrity))

    def reset_index(self, drop=False, level=None, inplace=False, col_level=0, col_fill=''):
        pass

    @derived_from(pd.DataFrame)
    def drop(self, labels=None, axis=0, index=None, columns=None, level=None,
             inplace=False, errors='raise'):
        return AbstractDataFrame(self.dataframe.drop(labels=labels,
                                                     axis=axis,
                                                     index=index,
                                                     columns=columns,
                                                     level=level,
                                                     inplace=inplace,
                                                     errors=errors))

    def from_records(self, data, index=None, exclude=None, columns=None, coerce_float=False, nrows=None, is_cache=True):
        pass

    def from_json(self, path, is_cache=True):
        pass

    def from_csv(self, path, header=True, sep=',', index_col=0, parse_dates=True, encoding=None, tupleize_cols=False,
                 infer_datetime_format=False, is_cache=True):
        pass

    def from_array(self, data, cols=None, is_cache=True):
        pass

    def from_dict(self, data, orient='columns', dtype=None, is_cache=True):
        pass

    @derived_from(pd.DataFrame)
    def to_csv(self, path_or_buf=None, sep=",", na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='w', encoding=None, compression=None, quoting=None, quotechar='"',
               line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        pass

    def to_json(self, path_or_buf=None, orient=None, date_format=None,
                double_precision=10, force_ascii=True, date_unit='ms',
                default_handler=None, lines=False, compression=None):
        pass

    @derived_from(pd.DataFrame)
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
        pass

    def sort_values(self, by=None, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        return AbstractDataFrame(self.dataframe.sort_values(by=by,
                                   axis=axis,
                                   ascending=ascending,
                                   inplace=inplace,
                                   kind=kind,
                                   na_position=na_position
                                   ))

    def filter(self, items=None, like=None, regex=None, axis=None):
        return AbstractDataFrame(self.dataframe.filter(items=items, like=like, regex=regex, axis=axis))

    def drop_duplicates(self, subset=None, keep='first', inplace=False):
        return AbstractDataFrame(self.dataframe.drop_duplicates(subset=subset, keep=keep, inplace=inplace))

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False,
              suffixes=('_x', '_y'), copy=True, indicator=False, validate=None):
        return AbstractDataFrame(self.dataframe.merge(right=right.dataframe, how=how, on=on, left_on=left_on, right_on=right_on,
                             left_index=left_index, right_index=right_index,
                             suffixes=suffixes, copy=copy, indicator=indicator, validate=validate))

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        return AbstractDataFrame(self.dataframe.join(other=other.dataframe, on=on, how=how, lsuffix=lsuffix, rsuffix=rsuffix))

    def mean(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        return AbstractDataFrame(self.dataframe.mean(axis=axis, skipna=skipna, level=level, numeric_only=numeric_only))

    def sum(self, axis=0, skipna=True, level=None, numeric_only=None, **kwargs):
        return AbstractDataFrame(self.dataframe.sum(axis=axis, skipna=skipna, level=level, numeric_only=numeric_only))

    def min(self, axis=0, skipna=True, level=None, numeric_only=None, **kwargs):
        return AbstractDataFrame(self.dataframe.min(axis=axis, skipna=skipna, level=level, numeric_only=numeric_only))

    def max(self, axis=0, skipna=True, level=None, numeric_only=None, **kwargs):
        return AbstractDataFrame(self.dataframe.max(axis=axis, skipna=skipna, level=level, numeric_only=numeric_only))

    def count(self, axis=0, level=None, numeric_only=None, **kwargs):
        return AbstractDataFrame(self.dataframe.count(axis=axis, level=level, numeric_only=numeric_only))

    def describe(self, percentiles=None, include=None, exclude=None):
        pass

    def tail(self, n):
        return AbstractDataFrame(self.dataframe.tail(n))

    def head(self, n):
        return AbstractDataFrame(self.dataframe.head(n))

    def groupbymin(self, groupby_obj):
        return AbstractDataFrame(self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).min())

    def groupbymax(self, groupby_obj):
        return AbstractDataFrame(self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).max())

    def groupbymean(self, groupby_obj):
        return AbstractDataFrame(self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).mean())

    def groupbysum(self, groupby_obj):
        return AbstractDataFrame(self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).sum())

    def groupbycount(self, groupby_obj):
        return AbstractDataFrame(self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).count())