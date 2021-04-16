#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
from Octopus.dataframe.core.sparkSeries import Series
from Octopus.dataframe.core.utils import derived_from


def groupby(obj, by, as_index, sort, **kwargs):
    if isinstance(obj, Series):
        klass = SeriesGroupBy
    elif isinstance(obj, SparkDataFrame):
        klass = DataFrameGroupBy
    else:  # pragma: no cover
        raise TypeError('invalid type: %s' % type(obj))

    return klass(obj, by, as_index=as_index, sort=sort, **kwargs)


class _GroupBy(object):
    """ Superclass for DataFrameGroupBy and SeriesGroupBy

    Parameters
    ----------

    obj: SparkDataFrame or Series
        DataFrame or Series to be grouped
    by: str, list or Series
        The key for grouping
    slice: str, list
        The slice keys applied to GroupBy result
    """

    def __init__(self, obj, keys=None, axis=0, level=None, as_index=True,
                 sort=True, group_keys=True, squeeze=False, **kwargs):
        assert isinstance(obj, (SparkDataFrame, Series))
        self.obj = obj
        self.keys = keys if isinstance(keys, list) else [keys]

        self.sort = sort
        self.group_keys = group_keys
        self.squeeze = squeeze
        self.as_index = as_index
        if not self.obj.use_pdf:
            if self.obj.index_col is not None:
                self.sdf = self.obj.sdf.drop(self.obj.index_col)
            else:
                self.sdf = self.obj.sdf
        else:
            self.pandas_groupby = self.obj._pdata.groupby(keys)

    def __getitem__(self, keys):
        if not self.obj.use_pdf:
            if isinstance(keys, tuple):
                keys = list(keys)
            if isinstance(keys, list):
                    keys = self.keys + keys
            else:
                keys = self.keys + [keys]

            self.sdf = self.sdf.select(keys)
        return self

    @derived_from(pd.core.groupby.GroupBy)
    def min(self):
        if self.obj.use_pdf:
            return SparkDataFrame(pdf=self.pandas_groupby.min())

        arg = {}
        for key in self.sdf.columns:
            if key not in self.keys:
                arg[key] = "min"
        return self.agg(arg)

    @derived_from(pd.core.groupby.GroupBy)
    def max(self):
        if self.obj.use_pdf:
            return SparkDataFrame(pdf=self.pandas_groupby.max())

        arg = {}
        for key in self.sdf.columns:
            if key not in self.keys:
                arg[key] = "max"
        return self.agg(arg)

    @derived_from(pd.core.groupby.GroupBy)
    def count(self):
        if self.obj.use_pdf:
            return SparkDataFrame(pdf=self.pandas_groupby.count())

        arg = {}
        for key in self.sdf.columns:
            if key not in self.keys:
                arg[key] = "count"
        return self.agg(arg)

    @derived_from(pd.core.groupby.GroupBy)
    def mean(self):
        if self.obj.use_pdf:
            return SparkDataFrame(pdf=self.pandas_groupby.mean())

        ret = self.sdf.groupby(self.keys).mean()
        if isinstance(self.keys, list):
            for key in self.keys:
                ret = ret.drop("avg(" + key + ")")
        for column in ret.columns:
            if column not in self.keys:
                ret = ret.withColumnRenamed(column, column[4:-1])

        if self.sort:
            ret = ret.sort(self.keys)

        if self.as_index:
            return SparkDataFrame(ret, self.keys)

        return SparkDataFrame(ret)

    @derived_from(pd.core.groupby.GroupBy)
    def sum(self):
        if self.obj.use_pdf:
            return SparkDataFrame(pdf=self.pandas_groupby.sum())

        ret = self.sdf.groupby(self.keys).sum()

        for key in self.keys:
            ret = ret.drop("sum(" + key + ")")
        for column in ret.columns:
            if column not in self.keys:
                ret = ret.withColumnRenamed(column, column[4:-1])

        if self.sort:
            ret = ret.sort(self.keys)

        if self.as_index:
            return SparkDataFrame(sdf=ret, index_col=self.keys[0])
        return SparkDataFrame(ret)

    def size(self):
        pass

    @derived_from(pd.core.groupby.GroupBy)
    def var(self):
        pass

    @derived_from(pd.core.groupby.GroupBy)
    def std(self):
        pass

    def aggregate(self, arg, *args, **kwargs):
        if self.obj.use_pdf:
            return SparkDataFrame(pdf=self.pandas_groupby.agg(arg))

        if isinstance(arg, dict):
            ret = self.sdf.groupby(self.keys).agg(arg)

            name_dict = {}
            for key in arg:
                arg[key] = "avg" if arg[key] == "mean" else arg[key]
                name_dict[arg[key]+"("+key+")"] = key

            for column in ret.columns:
                if column not in self.keys:
                    ret = ret.withColumnRenamed(column, name_dict[column])

            if self.sort:
                ret = ret.sort(self.keys)

            if self.as_index:
                return SparkDataFrame(ret, self.keys, is_cache=False)

            return SparkDataFrame(ret, is_cache=False)
        else:
            "Only support dict input now."

    agg = aggregate

    def apply(self, func):
        pass


class DataFrameGroupBy(_GroupBy):
    #_token_prefix = 'dataframe-groupby-'

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      list(filter(pd.compat.isidentifier, self.obj.columns))))

    @derived_from(pd.core.groupby.DataFrameGroupBy)
    def aggregate(self, arg, *args, **kwargs):
        if arg == 'size':
            return self.size()

        return super(DataFrameGroupBy, self).aggregate(arg)

    agg = aggregate


class SeriesGroupBy(_GroupBy):
    pass
