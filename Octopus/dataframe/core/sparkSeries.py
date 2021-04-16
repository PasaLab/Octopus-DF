#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from Octopus.dataframe.core.frame import Frame, spark
from Octopus.dataframe.core.utils import derived_from, exe_time

__all__ = ['Series']


class Series(Frame):
    def __init__(self, sdf=None, index_col=None, dtype=None, name=None, is_cache=True,
                 col=None, row=None, pdf=None, df_id=None, partition_info=None):
        """Octopus Series is a pandas-like interface focusing on big data processing under spark."""

        if self._check_valid_sdf(sdf, index_col, pdf):
            Frame.__init__(self, sdf, index_col, is_cache=is_cache, pdf=pdf, df_id=df_id,
                           partition_info=partition_info, is_df=False)
            self.col = col
            self.row = row
            if isinstance(self._pdata, pd.DataFrame):
                self._pdata = self._to_series(self._pdata)
            if name is not None:
                self._pdata.name = self.name = name
            else:
                self.name = self._pdata.name
            if dtype is not None:
                self.dtype = self._pdata.dtype = dtype
            else:
                self.dtype = self._pdata.dtype
            if self.col is None and self.sdf is not None and self.name in sdf.columns:
                self.col = sdf[self.name]

        else:
            raise Exception("Exception : Init failure, data is not a series!")

    @exe_time
    def _update_series(self, sdf=None, index_col=None, dtype=None, name=None, pdf=None):
        Frame.__update(self, sdf, index_col, pdf=pdf)
        if self._pdata is None or not isinstance(self._pdata, pd.Series):
            self._pdata = self._to_series(self._pdata)
        if name is not None:
            self._pdata.name = self.name = name
        else:
            self._pdata.name = self.name

    def _check_valid_sdf(self, sdf, index, pdf=None):
        if sdf is not None:
            if len([x for x in sdf.columns if x != index]) == 1:
                return True
            else:
                return sdf.count() == 1
        elif pdf is not None:
            return isinstance(pdf, pd.Series)
        else:
            raise Exception("Series init sdf should not be none!")

    def apply(self, func, convert_dtype=True, args=(), **kwds):
        pass

    def aggregate(self, func, axis=0, *args, **kwargs):
        pass

    agg = aggregate

    def rename(self, index=None, **kwargs):
        pass

    @exe_time
    @classmethod
    def from_pandas(cls, ps, index_col=None, is_cache=False, col=None):
        """Construct a Octopus Series from a Pandas Series,convert all data to str"""
        sdf = spark.createDataFrame(ps.to_frame())
        return Series(sdf, index_col=index_col, is_cache=is_cache, col=col)

    @exe_time
    @classmethod
    def from_csv(cls, path, header=True):
        """Construct a Octopus SparkDataFrame from a Pandas SparkDataFrame"""
        sdf = spark.read.csv(path, header=header)
        try:
            return Series(sdf)
        except:
            raise TypeError("Cannot be read as series.")

    @exe_time
    @derived_from(pd.Series)
    def to_dict(self, into=dict):
        """
        Convert Series to {label -> value} dict or dict-like object.

        Parameters
        ----------
        into : class, default dict
            The collections.Mapping subclass to use as the return
            object. Can be the actual class or an empty
            instance of the mapping type you want.  If you want a
            collections.defaultdict, you must pass it initialized.
        """
        return self._pdata.to_dict(into=into)

    @exe_time
    def to_frame(self, name=None):
        """
        Convert Series to SparkDataFrame

        Parameters
        ----------
        name : Not support.

        Returns
        -------
        data_frame : SparkDataFrame
        """
        from Octopus.dataframe.core import SparkDataFrame
        return SparkDataFrame(self.sdf)

    def to_xarray(self):
        pass

    @classmethod
    def _to_series(cls, pdata):
        if not isinstance(pdata, (pd.Series, pd.DataFrame)):
            raise Exception("pdata is not instance of pd.Series")

        if isinstance(pdata, pd.Series):
            return pdata
        elif pdata.shape[0] == 1:
            return pdata.iloc[0, :]
        elif pdata.shape[1] == 1:
            return pdata.iloc[:, 0]
        else:
            raise Exception("pdata is not Series")

    @classmethod
    def from_array(cls, data, index=None, col=None, name=None):
        """Construct Series from array."""
        ps = pd.Series(data, index=index, name=name)
        return Series.from_pandas(ps, col=col)

    @exe_time
    def tolist(self):
        return self._pdata.tolist()

    @exe_time
    @derived_from(pd.Series)
    def head(self, n=5):
        row_count = self.record_nums()
        if row_count > 1:
            if n > row_count:
                return Series(self.sdf, index_col=self.index_col)
            else:
                return Series(spark.createDataFrame(self.sdf.take(n)), index_col=self.index_col)
        else:
            col_count = len(self.columns)
            if n > col_count:
                return Series(self.sdf, index_col=self.index_col, is_cache=False)
            else:
                return Series(self.iloc[:, :n].sdf, index_col=self.index_col, is_cache=False)

    @exe_time
    @derived_from(pd.Series)
    def tail(self, n=5):
        row_count = self.record_nums()
        if row_count > 1:
            if n > row_count:
                return Series(self.sdf, index_col=self.index_col)
            else:
                return Series(self.iloc[row_count - n:].sdf, index_col=self.index_col)
        else:
            col_count = len(self.columns)
            if n > col_count:
                return Series(self.sdf, index_col=self.index_col)
            else:
                return Series(self.iloc[:, col_count-n:].sdf, index_col=self.index_col)

    @exe_time
    @property
    @derived_from(pd.Series)
    def shape(self):
        if len(self.columns) != 1:
            return len(self.columns),
        else:
            return self.record_nums()

    @exe_time
    def min(self, axis=None, skipna=True, split_every=False):
        """
        Return min of the series

        Parameters
        ----------
        axis : Not support.
        skipna : Not support.
        split_every : Not support.
        """
        d = {}
        cols_count = len(list(self.columns))
        if cols_count > 1:
            pass
        else:
            col = list(self.columns)[0]
            d[col] = 'min'
            col = 'min(' + col + ')'
            return self.sdf.agg(d).collect()[0][col]

    @exe_time
    def max(self, axis=None, skipna=True, split_every=False):
        """
        Return max of the series

        Parameters
        ----------
        axis : Not support.
        skipna : Not support.
        split_every : Not support.
        """
        d = {}
        cols_count = len(list(self.columns))
        if cols_count > 1:
            pass
        else:
            col = list(self.columns)[0]
            d[col] = 'max'
            col = 'max('+col+')'
            return self.sdf.agg(d).collect()[0][col]

    @exe_time
    def mean(self, axis=None, skipna=True, split_every=False):
        """
        Return mean of the series

        Parameters
        ----------
        axis : Not support.
        skipna : Not support.
        split_every : Not support.
        """
        d = {}
        cols_count = len(list(self.columns))
        if cols_count > 1:
            pass
        else:
            col = list(self.columns)[0]
            d[col] = 'avg'
            col = 'avg(' + col + ')'
            return self.sdf.agg(d).collect()[0][col]

    @exe_time
    def sum(self, axis=None, skipna=True, split_every=False):
        """
        Return sum of the series

        Parameters
        ----------
        axis : Not support.
        skipna : Not support.
        split_every : Not support.
        """
        d = {}
        cols_count = len(list(self.columns))
        if cols_count > 1:
            pass
        else:
            col = list(self.columns)[0]
            d[col] = 'sum'
            col = 'sum(' + col + ')'
            return self.sdf.agg(d).collect()[0][col]

    @exe_time
    def median(self, axis=None, skipna=True, split_every=False):
        pass

    @classmethod
    def both_pdf(cls, A, B):
        if isinstance(A, Frame) and isinstance(B, Frame):
            return A.use_pdf and B.use_pdf
        else:
            raise Exception("invalid parameter, not instance of Octopus.dataframe.core.Frame")

    def __eq__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__eq__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__eq__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x == other for x in self._pdata.tolist()], col=(self.col == other))
        elif isinstance(other, Series):
            return self.from_array([x == y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col == other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __ne__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__ne__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__ne__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x != other for x in self._pdata.tolist()], col=(self.col != other))
        elif isinstance(other, Series):
            return self.from_array([x != y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col != other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __le__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__le__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__le__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x <= other for x in self._pdata.tolist()], col=(self.col <= other))
        elif isinstance(other, Series):
            return self.from_array([x <= y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col <= other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __lt__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__lt__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__lt__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x < other for x in self._pdata.tolist()], col=(self.col < other))
        elif isinstance(other, Series):
            return self.from_array([x < y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col < other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __ge__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__ge__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__ge__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x >= other for x in self._pdata.tolist()], col=(self.col >= other))
        elif isinstance(other, Series):
            return self.from_array([x >= y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col >= other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __gt__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__gt__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__gt__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x > other for x in self._pdata.tolist()], col=(self.col > other))
        elif isinstance(other, Series):
            return self.from_array([x > y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col > other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __and__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__and__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__and__(other))

        if isinstance(other, (str, int, float, bool)):
            return self.from_array([x and other for x in self._pdata.tolist()], col=(self.col & other))
        elif isinstance(other, Series):
            return self.from_array([x and y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col & other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __or__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__or__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__or__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x or other for x in self._pdata.tolist()], col=(self.col | other))
        elif isinstance(other, Series):
            return self.from_array([x or y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col | other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __add__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__add__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__add__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x + other for x in self._pdata.tolist()], col=(self.col + other))
        elif isinstance(other, Series):
            return self.from_array([x + y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col + other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __sub__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__sub__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__sub__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x - other for x in self._pdata.tolist()], col=(self.col - other))
        elif isinstance(other, Series):
            return self.from_array([x - y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col - other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __mul__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__mul__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__mul__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x * other for x in self._pdata.tolist()], col=(self.col * other))
        elif isinstance(other, Series):
            return self.from_array([x * y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col * other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __truediv__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__truediv__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__truediv__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x / other for x in self._pdata.tolist()], col=(self.col / other))
        elif isinstance(other, Series):
            return self.from_array([x / y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col / other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __floordiv__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__floordiv__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__floordiv__(other))

        if isinstance(other, (str, int, float)):
            return self.from_array([x // other for x in self._pdata.tolist()])
        elif isinstance(other, Series):
            return self.from_array([x // y for x, y in zip(self._pdata.tolist(), other.tolist())])
        else:
            raise Exception("Not support " + str(type(other)))

    def __pow__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__pow__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__pow__(other))

        if isinstance(other, (str, int)):
            return self.from_array([x ** other for x in self._pdata.tolist()], col=(self.col ** other))
        elif isinstance(other, Series):
            return self.from_array([x ** y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col ** other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __mod__(self, other):
        if isinstance(other, Series) and self.both_pdf(self, other):
            return Series(pdf=self._pdata.__mod__(other._pdata))
        elif self.use_pdf and isinstance(other, (str, int, float)):
            return Series(pdf=self._pdata.__mod__(other))

        if isinstance(other, (str, int)):
            return self.from_array([x % other for x in self._pdata.tolist()], col=(self.col % other))
        elif isinstance(other, Series):
            return self.from_array([x % y for x, y in zip(self._pdata.tolist(), other.tolist())],
                                   col=(self.col % other.col))
        else:
            raise Exception("Not support " + str(type(other)))

    def __iter__(self):
        return self._pdata.__iter__()

