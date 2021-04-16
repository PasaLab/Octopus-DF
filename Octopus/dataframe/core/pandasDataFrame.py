#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandas import DataFrame

from Octopus.dataframe.core.abstractDataFrame import AbstractDataFrame

__all__ = ['PandasDataFrame']


class PandasDataFrame(AbstractDataFrame):
    def __init__(self, data=None, index=None, columns=None, dtype=None,
                 copy=False):
        if type(data) is DataFrame:
            self.dataframe = data
        else:
            self.dataframe = DataFrame(data=data, index=index, columns=columns, dtype=dtype, copy=copy)

    def groupbymin(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).min()

    def groupbymax(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).max()

    def groupbymean(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).mean()

    def groupbysum(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).sum()

    def groupbycount(self, groupby_obj):
        return self.dataframe.groupby(by=groupby_obj.by, axis=groupby_obj.axis, level=groupby_obj.level,
                                      as_index=groupby_obj.as_index, sort=groupby_obj.sort,
                                      group_keys=groupby_obj.group_keys,
                                      squeeze=groupby_obj.squeeze).count()


def read_csv_pandas(filepath_or_buffer,
                    sep=',',
                    delimiter=None,
                    header='infer',
                    names=None,
                    index_col=None,
                    usecols=None,
                    squeeze=False):
    import pandas as pd
    return PandasDataFrame(data=pd.read_csv(filepath_or_buffer=filepath_or_buffer,
                                            sep=sep,
                                            delimiter=delimiter,
                                            header=header,
                                            names=names,
                                            index_col=index_col,
                                            usecols=usecols,
                                            squeeze=squeeze))