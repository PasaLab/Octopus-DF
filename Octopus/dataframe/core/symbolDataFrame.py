#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pyarrow as pa
import dask.dataframe as dd
from dask.distributed import Client

from Octopus.dataframe.core.utils import derived_from, exe_time, IdGenerator
from Octopus.dataframe.predict.getPerformance import get_operator_time_by_size, get_trans_time_by_size

#
# client = Client(n_workers=4, threads_per_worker=24, processes=True, memory_limit="25GB")
# fs = pa.hdfs.connect(user="experiment")

id_df_map = {}
operator_time = {'Dask': {}, 'Spark': {}, 'Pandas': {}}
platforms = ['Dask', 'Pandas', 'Spark']
cur_max_id = IdGenerator.get_id()
base_line_time = 100000000000000000.0
optimized_id_platform_map = {}
MAX_TRANS_CNT = 4


class SymbolLocIndex(object):
    def __init__(self, obj, engine_type=None):
        self.args = None
        self.obj = obj
        self.size = None
        self.engine_type = engine_type

    def __getitem__(self, key):
        # Todo：如果loc切片的标签不在采样结果中，那么无解（按照正常情况拥有来处理，即按照所有数据处理，单标签查询或list查询为宜）
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            df_next = self.obj.df_sample.loc[key]
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="loc",
                                   id=id,
                                   key=key,
                                   subfunc="__getitem__",
                                   df_sample=df_next,
                                   rate=self.obj.rate,
                                   size=self.obj.size,
                                   engine_type=self.engine_type
                                   )
        else:
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="loc",
                                   subfunc="__getitem__",
                                   id=id,
                                   key=key,
                                   engine_type=self.engine_type
                                   )

    def __setitem__(self, key, value):
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            df_next = self.obj.df_sample.loc[key]
            id_df_map[self.obj.id].update_queue.append(
                SymbolDataFrame(prev=[self.obj.id],
                                func="loc",
                                id=id,
                                subfunc="__setitem__",
                                key=key,
                                value=value,
                                df_sample=df_next,
                                rate=self.obj.rate,
                                size=self.obj.size,
                                engine_type=self.engine_type
                                )
            )
        else:
            id_df_map[self.obj.id].update_queue.append(
                SymbolDataFrame(prev=[self.obj.id],
                                func="loc",
                                id=id,
                                subfunc="__setitem__",
                                key=key,
                                value=value,
                                engine_type=self.engine_type
                                )
            )


class SymbolILocIndex(object):
    def __init__(self, obj, engine_type=None):
        self.args = None
        self.obj = obj
        self.size = None
        self.engine_type = engine_type

    def __getitem__(self, key):
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            import pandas.core.common as com
            key = tuple(com.apply_if_callable(x, self.obj) for x in key)

            start1 = 0 if key[0].start is None else int(key[0].start * self.obj.rate[0])
            stop1 = int(self.obj.size[0] * self.obj.rate[0]) if key[0].stop is None else int(key[0].stop * self.obj.rate[0])
            step1 = key[0].step

            start2 = 0 if key[1].start is None else int(key[1].start * self.obj.rate[1])
            stop2 = int(self.obj.size[1] * self.obj.rate[1]) if key[1].stop is None else int(key[1].stop * self.obj.rate[1])
            step2 = key[1].step

            df_next = self.obj.df_sample.iloc[start1:stop1:step1, start2:stop2:step2]
            return SymbolDataFrame(prev=[self.obj.id],
                               func="iloc",
                               id=id,
                               subfunc="__getitem__",
                               key=key,
                               df_sample=df_next,
                               rate=self.obj.rate,
                               size=self.obj.size,
                               engine_type=self.engine_type
                               )
        else:
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="iloc",
                                   subfunc="__getitem__",
                                   id=id,
                                   key=key,
                                   engine_type=self.engine_type
                                   )

    def __setitem__(self, key, value):
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            import pandas.core.common as com
            key = tuple(com.apply_if_callable(x, self.obj) for x in key)
            start1 = 0 if key[0].start is None else int(key[0].start * self.obj.rate[0])
            stop1 = int(self.obj.size[0] * self.obj.rate[0]) if key[0].stop is None else int(
                key[0].stop * self.obj.rate[0])
            step1 = key[0].step

            start2 = 0 if key[1].start is None else int(key[1].start * self.obj.rate[1])
            stop2 = int(self.obj.size[1] * self.obj.rate[1]) if key[1].stop is None else int(
                key[1].stop * self.obj.rate[1])
            step2 = key[1].step

            df_next = self.obj.df_sample.iloc[start1:stop1:step1, start2:stop2:step2]
            id_df_map[self.obj.id].update_queue.append(
                SymbolDataFrame(prev=[self.obj.id],
                                func="iloc",
                                id=id,
                                subfunc="__setitem__",
                                key=key,
                                value=value,
                                df_sample=df_next,
                                rate=self.obj.rate,
                                size=self.obj.size,
                                engine_type=self.engine_type
                                )
            )
        else:
            id_df_map[self.obj.id].update_queue.append(
                SymbolDataFrame(prev=[self.obj.id],
                                func="iloc",
                                id=id,
                                subfunc="__setitem__",
                                key=key,
                                value=value,
                                engine_type=self.engine_type
                                )
            )


class SymbolGroupby(object):
    def __init__(self, obj, by=None, axis=0, level=None, as_index=True,
                 sort=True, group_keys=True, squeeze=False, **kwargs):
        self.obj = obj
        self.by = by
        self.sort = sort
        self.as_index = as_index
        self.axis = axis
        self.level = level
        self.as_index = as_index
        self.group_keys = group_keys
        self.squeeze = squeeze
        self.rate = self.obj.rate
        self.kwargs = kwargs
        self.engine_type = self.obj.engine_type
        self.groupby_sample = None
        if self.engine_type is None:
            self.groupby_sample = self.obj.df_sample.groupby(by=by, sort=sort, as_index=as_index)

    # def __getitem__(self, keys):
    #     return self

    def min(self):
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            df_next = self.groupby_sample.min()
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbymin",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   groupby_obj=self
                                   )
        else:
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbymin",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   engine_type=self.engine_type,
                                   groupby_obj=self
                                   )

    def max(self):
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            df_next = self.groupby_sample.max()
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbymax",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   groupby_obj=self
                                   )
        else:
            return SymbolDataFrame(prev=[self.obj.id], func="groupbymax", id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   engine_type=self.engine_type,
                                   groupby_obj=self
                                   )

    def count(self):
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            df_next = self.groupby_sample.count()
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbycount",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   groupby_obj=self
                                   )
        else:
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbycount",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   engine_type=self.engine_type,
                                   groupby_obj=self
                                   )

    def mean(self):
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            df_next = self.groupby_sample.mean()
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbymean",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   groupby_obj=self
                                   )
        else:
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbymean",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   engine_type=self.engine_type,
                                   groupby_obj=self
                                   )

    def sum(self):
        id = IdGenerator.next_id()
        self.obj.children.append(id)
        if self.engine_type is None:
            df_next = self.groupby_sample.sum()
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbysum",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   groupby_obj=self
                                   )
        else:
            return SymbolDataFrame(prev=[self.obj.id],
                                   func="groupbysum",
                                   id=id,
                                   by=self.by,
                                   sort=self.sort,
                                   as_index=self.as_index,
                                   engine_type=self.engine_type,
                                   groupby_obj=self
                                   )


class SymbolDataFrame(object):
    def __init__(self, prev=[], func=None, id=None, **kwargs):
        self.prev = prev
        self.func = func
        self.id = id
        self.cols = None
        self.children = []
        self.kwargs = kwargs
        self.df_sample = kwargs.get('df_sample', None)
        self.size = kwargs.get('size', None)
        self.rate = kwargs.get('rate', (1, 1))
        self.engine_type = kwargs.get('engine_type', None)
        optimized_id_platform_map[id] = self.engine_type
        self.platforms = {'Dask': None, 'Spark': None, 'Pandas': None}
        self.orders = None
        id_df_map[id] = self
        self.update_queue = []
        self.dag_exist_common_ancestor = True

    def __getitem__(self, key):
        id = IdGenerator.next_id()
        self.children.append(id)
        if self.engine_type is None:
            df_next = self.df_sample[key]
            return SymbolDataFrame(prev=[self.id],
                                   func="__getitem__",
                                   id=id,
                                   key=key,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=self.size,
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="__getitem__",
                                   id=id,
                                   key=key,
                                   engine_type=self.engine_type
                                   )

    def __setitem__(self, key, value):
        id = IdGenerator.next_id()
        self.children.append(id)
        if self.engine_type is None:
            self.df_sample[key] = value
            id_df_map[self.id].update_queue.append(
                SymbolDataFrame(prev=[self.id],
                                func="__setitem__",
                                id=id,
                                key=key,
                                value=value,
                                df_sample=self.df_sample,
                                rate=self.rate,
                                size=self.size,
                                )
            )
        else:
            id_df_map[self.id].update_queue.append(
                SymbolDataFrame(prev=[self.id],
                                func="__setitem__",
                                id=id,
                                key=key,
                                value=value,
                                engine_type=self.engine_type
                                )
            )

    @property
    def loc(self):
        return SymbolLocIndex(self, engine_type=self.engine_type)

    @property
    def iloc(self):
        return SymbolILocIndex(self, engine_type=self.engine_type)

    def drop(self, labels=None, axis=0, index=None, columns=None, level=None,
             inplace=False, errors='raise'):
        id = IdGenerator.next_id()
        self.children.append(id)
        if self.engine_type is None:
            df_next = self.df_sample.drop(labels=labels,
                                          axis=axis,
                                          index=index,
                                          columns=columns,
                                          level=level,
                                          inplace=inplace,
                                          errors=errors)
            return SymbolDataFrame(prev=[self.id],
                                   func="drop",
                                   id=id,
                                   labels=labels,
                                   axis=axis,
                                   index=index,
                                   columns=columns,
                                   level=level,
                                   inplace=inplace,
                                   errors=errors,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=self.size,
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="drop",
                                   id=id,
                                   labels=labels,
                                   axis=axis,
                                   index=index,
                                   columns=columns,
                                   level=level,
                                   inplace=inplace,
                                   errors=errors,
                                   engine_type=self.engine_type
                                   )

    def from_records(self, data, index=None, exclude=None, columns=None, coerce_float=False, nrows=None, is_cache=True):
        pass

    def from_json(self, path, is_cache=True):
        pass

    def from_pandas(self, pdf, is_cache=True):
        pass

    @classmethod
    def from_csv(cls, path, header=True, sep=',', index_col=0, parse_dates=True, encoding=None, tupleize_cols=False,
                 infer_datetime_format=False, is_cache=True, engine_type=None):

        # Todo：计算出行列大小size，判断是否需要采样，计算出rate，df_sample, 初始DataFrame使用Spark读（根据路径）
        rate = (0.01, 1)
        # Todo：此处可以根据函数库获取hdfs上数据的具体信息
        # from Octopus.dataframe.core.sparkinit import spark
        # sdf = spark.read.csv(path, header=True, inferSchema=True).repartition(72)
        # size = (sdf.count(), len(sdf.columns))
        if engine_type is None:
            pdf = None
            if path[0:4] == 'file':
                import pandas as pd
                pdf = pd.read_csv(path)
            else:
                pdf = dd.read_csv(path).compute()
            if 8000 > len(pdf.index):
                rate = (1, 1)
                df_sample = pdf
            else:
                rate = (8000.0 / float(len(pdf.index)), 1)
            if rate[0] != 1:
                df_sample = pdf.sample(frac=rate[0])

            id = IdGenerator.next_id()
            return SymbolDataFrame(prev=[],
                                   func="from_csv",
                                   id=id,
                                   path=path,
                                   header=header,
                                   sep=sep,
                                   index_col=index_col,
                                   parse_dates=parse_dates,
                                   encoding=encoding,
                                   tupleize_cols=tupleize_cols,
                                   infer_datetime_format=infer_datetime_format,
                                   size=(len(pdf.index), len(pdf.columns)),
                                   df_sample=df_sample,
                                   rate=rate,
                                   pandas_df=pdf,
                                   is_cache=is_cache,
                                   engine_type=engine_type)
        else:
            id = IdGenerator.next_id()
            return SymbolDataFrame(prev=[],
                                   func="from_csv",
                                   id=id,
                                   path=path,
                                   header=header,
                                   sep=sep,
                                   index_col=index_col,
                                   parse_dates=parse_dates,
                                   encoding=encoding,
                                   tupleize_cols=tupleize_cols,
                                   infer_datetime_format=infer_datetime_format,
                                   is_cache=is_cache,
                                   engine_type=engine_type)

    def from_dict(cls, data, orient='columns', dtype=None, is_cache=True):
        pass

    def to_csv(self, path_or_buf=None, sep=",", na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='w', encoding=None, compression=None, quoting=None, quotechar='"',
               line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):

        id = IdGenerator.next_id()
        self.children.append(id)

        return SymbolDataFrame(prev=[self.id],
                               func="to_csv",
                               id=id,
                               path=path_or_buf,
                               sep=sep,
                               na_rep=na_rep,
                               float_format=float_format,
                               columns=columns,
                               header=header,
                               index=index)

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

        return SymbolGroupby(self,
                             by=by,
                             axis=axis,
                             level=level,
                             as_index=as_index,
                             sort=sort,
                             group_keys=group_keys,
                             squeeze=squeeze,
                             kwargs=kwargs)

    def sort_values(self, by=None, axis=0, ascending=True,
                    kind='quicksort', na_position='last'):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.sort_values(by=by,
                                                 axis=axis,
                                                 ascending=ascending,
                                                 inplace=False,
                                                 kind=kind,
                                                 na_position=na_position)
            return SymbolDataFrame(prev=[self.id],
                                   func="sort_values",
                                   id=id,
                                   by=by,
                                   axis=axis,
                                   ascending=ascending,
                                   inplace=False,
                                   kind=kind,
                                   na_position=na_position,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1])
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="sort_values",
                                   id=id,
                                   by=by,
                                   axis=axis,
                                   ascending=ascending,
                                   inplace=False,
                                   kind=kind,
                                   na_position=na_position,
                                   engine_type=self.engine_type
                                   )

    def filter(self, items=None, like=None, regex=None, axis=None):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.filter(items=items, like=like, regex=regex, axis=axis)
            return SymbolDataFrame(prev=[self.id],
                                   func="filter",
                                   id=id,
                                   items=items,
                                   like=like,
                                   regex=regex,
                                   axis=axis,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]))
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="filter",
                                   id=id,
                                   items=items,
                                   like=like,
                                   regex=regex,
                                   axis=axis,
                                   engine_type=self.engine_type
                                   )

    def drop_duplicates(self, subset=None, keep='first', inplace=False):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.drop_duplicates(subset=subset, keep=keep, inplace=inplace)
            return SymbolDataFrame(prev=[self.id],
                                   func="drop_duplicates",
                                   id=id,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   subset=subset,
                                   keep=keep,
                                   inplace=inplace)
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="drop_duplicates",
                                   id=id,
                                   subset=subset,
                                   keep=keep,
                                   inplace=inplace,
                                   engine_type=self.engine_type)

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False,
              suffixes=('_x', '_y'), copy=True, indicator=False, validate=None):
        id = IdGenerator.next_id()
        self.children.append(id)
        right.children.append(id)
        if self.engine_type is None:
            df_next = self.df_sample.merge(right.df_sample,
                                           right=right,
                                           how=how,
                                           on=on,
                                           left_on=left_on,
                                           right_on=right_on,
                                           left_index=left_index,
                                           right_index=right_index,
                                           suffixes=suffixes,
                                           copy=copy,
                                           indicator=indicator,
                                           validate=validate,
                                           )
            return SymbolDataFrame(prev=[self.id, right.id], func="merge", id=id,
                                   right=right,
                                   how=how,
                                   on=on,
                                   left_on=left_on,
                                   right_on=right_on,
                                   left_index=left_index,
                                   right_index=right_index,
                                   suffixes=suffixes,
                                   copy=copy,
                                   indicator=indicator,
                                   validate=validate,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   )
        else:
            return SymbolDataFrame(prev=[self.id, right.id], func="merge", id=id,
                                   right=right,
                                   how=how,
                                   on=on,
                                   left_on=left_on,
                                   right_on=right_on,
                                   left_index=left_index,
                                   right_index=right_index,
                                   suffixes=suffixes,
                                   copy=copy,
                                   indicator=indicator,
                                   validate=validate,
                                   engine_type=self.engine_type
                                   )

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        id = IdGenerator.next_id()
        self.children.append(id)
        other.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.join(other=other.df_sample,
                                          on=on,
                                          how=how,
                                          lsuffix=lsuffix,
                                          rsuffix=rsuffix,
                                          sort=sort)
            return SymbolDataFrame(prev=[self.id, other.id],
                                   func="join",
                                   id=id,
                                   other=other,
                                   on=on,
                                   how=how,
                                   lsuffix=lsuffix,
                                   rsuffix=rsuffix,
                                   sort=sort,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   )
        else:
            return SymbolDataFrame(prev=[self.id, other.id],
                                   func="join",
                                   id=id,
                                   other=other,
                                   on=on,
                                   how=how,
                                   lsuffix=lsuffix,
                                   rsuffix=rsuffix,
                                   sort=sort,
                                   engine_type=self.engine_type
                                   )





    def mean(self, axis=None, skipna=True, split_every=False):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.mean(axis=axis, skipna=skipna, split_every=split_every)
            return SymbolDataFrame(prev=[self.id],
                                   func="mean",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="mean",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   engine_type=self.engine_type
                                   )

    def sum(self, axis=0, skipna=True, split_every=False):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.sum(axis=axis, skipna=skipna, split_every=split_every)
            return SymbolDataFrame(prev=[self.id],
                                   func="sum",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="sum",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   engine_type=self.engine_type
                                   )

    def min(self, axis=0, skipna=True, split_every=False):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.min(axis=axis, skipna=skipna, split_every=split_every)
            return SymbolDataFrame(prev=[self.id],
                                   func="min",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="min",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   engine_type=self.engine_type
                                   )

    def max(self, axis=0, skipna=True, split_every=False):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.max(axis=axis, skipna=skipna, split_every=split_every)
            return SymbolDataFrame(prev=[self.id],
                                   func="max",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="max",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   engine_type=self.engine_type
                                   )

    def count(self, axis=0, skipna=True, split_every=False):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample.count(axis=axis, skipna=skipna, split_every=split_every)
            return SymbolDataFrame(prev=[self.id],
                                   func="count",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1]),
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="count",
                                   id=id,
                                   axis=axis,
                                   skipna=skipna,
                                   engine_type=self.engine_type
                                   )

    def set_index(self, keys, drop=True, append=False, verify_integrity=False,
                  use_secondary_index=False, use_local_index=False):
        id = IdGenerator.next_id()
        self.children.append(id)
        if self.engine_type is None:
            df_next = self.df_sample.set_index(keys=keys,
                                               drop=drop,
                                               append=append,
                                               inplace=False,
                                               verify_integrity=verify_integrity)
            return SymbolDataFrame(prev=[self.id],
                                   func="set_index",
                                   id=id,
                                   keys=keys,
                                   drop=drop,
                                   append=append,
                                   inplace=False,
                                   verify_integrity=verify_integrity,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1])
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="set_index",
                                   id=id,
                                   keys=keys,
                                   drop=drop,
                                   append=append,
                                   inplace=False,
                                   verify_integrity=verify_integrity,
                                   use_secondary_index=use_secondary_index,
                                   use_local_index=use_local_index,
                                   engine_type=self.engine_type
                                   )

    def describe(self, percentiles=None, include=None, exclude=None):
        if self.engine_type is None:
            df_next = self.df_sample.describe(percentiles=percentiles, include=include, exclude=exclude)
        else:
            df_next = None

        id = IdGenerator.next_id()
        self.children.append(id)
        return SymbolDataFrame(prev=[self.id],
                               func="describe",
                               id=id,
                               percentiles=percentiles,
                               include=include,
                               exclude=exclude,
                               df_sample=df_next,
                               rate=self.rate,
                               size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1])
                               )

    def tail(self, n):
        id = IdGenerator.next_id()
        self.children.append(id)
        if self.engine_type is None:
            df_next = self.df_sample
            if n > self.size[0]:
                num = int(n * self.rate[0])
                num = num if num > 1 else 1
                df_next = self.df_sample.tail(num)

            return SymbolDataFrame(prev=[self.id],
                                   func="tail",
                                   id=id,
                                   n=n,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1])
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="tail",
                                   id=id,
                                   n=n,
                                   engine_type=self.engine_type
                                   )

    def head(self, n):
        id = IdGenerator.next_id()
        self.children.append(id)

        if self.engine_type is None:
            df_next = self.df_sample
            if n > self.size[0]:
                num = int(n * self.rate[0])
                num = num if num > 1 else 1
                df_next = self.df_sample.head(num)

            return SymbolDataFrame(prev=[self.id],
                                   func="head",
                                   id=id,
                                   n=n,
                                   df_sample=df_next,
                                   rate=self.rate,
                                   size=(len(df_next.index) / self.rate[0], len(df_next.columns) / self.rate[1])
                                   )
        else:
            return SymbolDataFrame(prev=[self.id],
                                   func="head",
                                   id=id,
                                   n=n,
                                   engine_type=self.engine_type
                                   )

    def show(self):
        deps = self.prev
        order_stack = [(self.func, [self.id])]
        while deps is not None:
            level = []
            for dep in deps:
                level.append((dep.id, dep.func))
            order_stack.append(level)
            deps = deps.prev

        i = 0
        for level in order_stack:
            print("level: ", i, "func", level[1])

    def execute(self):
        platform = optimized_id_platform_map[self.id] if self.engine_type is None else self.engine_type
        prev_df = []
        if self.platforms[platform] is None:
            if self.func == "from_csv":
                if self.engine_type is None:
                    pdf = self.kwargs['pandas_df']
                    self.platforms["Pandas"] = pdf
                if platform == "Pandas":
                    from Octopus.dataframe.core.pandasDataFrame import read_csv_pandas
                    self.platforms["Pandas"] = read_csv_pandas(self.kwargs['path'])
                elif platform == "Dask":
                    from Octopus.dataframe.core.daskDataFrame import read_csv_dask
                    self.platforms["Dask"] = read_csv_dask(self.kwargs['path'])
                elif platform == "Spark":
                    from Octopus.dataframe.core.methods import read_csv_spark
                    self.platforms["Spark"] = read_csv_spark(self.kwargs['path'])
                else:
                    raise Exception("Not support platform ", platform)
            else:
                for id in self.prev:
                    df = id_df_map[id].platforms[platform]
                    if df is None:
                        raise Exception('Cannot get dataframe in platform', platform)
                    prev_df.append(df)

                import inspect
                if self.func == 'iloc' or self.func == 'loc':
                    key = self.kwargs['key']
                    subfunc = self.kwargs['subfunc']
                    if subfunc == '__setitem__':
                        value = self.kwargs['value']
                        getattr(prev_df[0], self.func).__setitem__(key=key, value=value)
                        cur_df = prev_df[0]
                    else:
                        cur_df = getattr(prev_df[0], self.func).__getitem__(key=key)
                else:
                    func = getattr(prev_df[0], self.func)
                    args = inspect.getfullargspec(func).args
                    kwargs = {}
                    for arg in args:
                        if arg in self.kwargs:
                            kwargs[arg] = self.kwargs[arg]

                    if len(prev_df) == 2:
                        if self.func == 'merge':
                            kwargs.pop('right', None)
                            cur_df = func(right=prev_df[1], **kwargs)
                        else:
                            kwargs.pop('other', None)
                            cur_df = func(other=prev_df[1], **kwargs)
                    else:
                        cur_df = func(**kwargs)
                        if self.func == '__setitem__':
                            cur_df = prev_df[0]

                self.platforms[platform] = cur_df

        if self.update_queue != []:
            for update_df in self.update_queue:
                update_df.execute()

            self.platforms[platform] = self.update_queue[-1].platforms[platform]
            self.update_queue = []

        for child in self.children:
            child_platform = optimized_id_platform_map[child]
            if self.platforms[child_platform] is None:
                self.transfer(self.platforms[platform], platform, child_platform)

    # 注意使用PandasDataFrame 而非原生Pandas
    def transfer(self, data, source, target):
        from Octopus.dataframe.io.io2hdfs import pandas_read_hdfs, pandas_write_hdfs, \
            dask_read_hdfs, dask_write_hdfs, spark_read_hdfs, spark_write_hdfs
        if source == 'Pandas':
            path = pandas_write_hdfs(data)
            if target == 'Spark':
                sdf = spark_read_hdfs(path)
                self.platforms[target] = sdf
            elif target == 'Dask':
                ddf = dask_read_hdfs(path)
                self.platforms[target] = ddf
            else:
                raise Exception("Not support target DataFrame platform", target)
        elif source == "Dask":
            path = dask_write_hdfs(data.dataframe)
            if target == "Pandas":
                self.platforms[target] = data.to_pandas()
            elif target == "Spark":
                sdf = spark_read_hdfs(path)
                self.platforms[target] = sdf
            else:
                raise Exception("Not support target DataFrame platform", target)
        elif source == "Spark":
            path = spark_write_hdfs(data.sdf)
            if target == "Pandas":
                pdf = pandas_read_hdfs(path)
                self.platforms[target] = pdf
            elif target == "Dask":
                ddf = dask_read_hdfs(path)
                self.platforms[target] = ddf
            else:
                raise Exception("Not support target DataFrame platform", target)
        else:
            raise Exception("Not support source DataFrame platform", source)

    def schedule(self):
        # print(optimized_id_platform_map)
        # for key in optimized_id_platform_map:
        #     optimized_id_platform_map[key] = 'Pandas'
        # print("final map", optimized_id_platform_map)
        # print("orders", self.orders)
        # import time
        # t0 = time.time()
        # time_list = []

        for df_id in self.orders:
            id_df_map[df_id].execute()
            # time_list.append(time.time() - t0)

        last_sdf = id_df_map[self.orders[-1]]
        if last_sdf.engine_type is not None:
            return last_sdf.platforms[last_sdf.engine_type]
        else:
            return last_sdf.platforms[optimized_id_platform_map[last_sdf.id]]

    def lineage_compress(self):
        self.continuous_iloc_lineage_compress()
        logging.debug("after lineage compress, the orders are {orders}".format(orders=self.orders))

    def continuous_iloc_lineage_compress(self):
        start = -1
        i = 0
        for df_id in self.orders:
            if id_df_map[df_id].func is 'iloc':
                if start == -1:
                    start = i
                elif id_df_map[df_id].prev[0] != self.orders[i-1]:
                    start = i
            else:
                if start != -1 and i - start > 1:
                    self.compress(start, i)

                start = -1
            i += 1

        if start != -1 and i - start > 1:
            self.compress(start, i)

    # 压缩完，会减少orders中的节点
    def compress(self, start, end):
        if start + 1 >= end:
            return
        key = id_df_map[self.orders[start]].kwargs['key']
        prev_id = id_df_map[self.orders[start]].prev[0]
        id_df_map.pop(self.orders[start])
        optimized_id_platform_map.pop(self.orders[start])
        tmp_orders = self.orders[:start:]

        for i in range(start + 1, end):
            cur_key = id_df_map[self.orders[i]].kwargs['key']
            key = (SymbolDataFrame.merge_iloc_key(key[0], cur_key[0]),
                   SymbolDataFrame.merge_iloc_key(key[1], cur_key[1]))
            # remove start to end from id_df_map
            id_df_map.pop(self.orders[i])
            optimized_id_platform_map.pop(self.orders[i])

        # add SymbolDataFrame which has new_key
        sdf = id_df_map[prev_id].iloc[key]
        id_df_map[sdf.id] = sdf
        tmp_orders.append(sdf.id)
        self.orders = tmp_orders + self.orders[end::]

    @classmethod
    def merge_iloc_key(cls, key1, key2):
        if type(key2) is not list and type(key2) is not slice:
            raise Exception("type key2 should be either list or slice")

        if type(key2) is list:
            new_key = []
            if type(key1) is list:
                for i in key2:
                    if i >= len(key1):
                        raise Exception("key out of index")
                    new_key.append(key1[i])
            elif type(key1) is slice:
                start = key1.start if key1.start is not None else 0
                step = key1.step if key1.step is not None else 1
                stop = key1.stop
                for i in key2:
                    ele = start + i * step
                    if stop is not None and ele >= stop:
                        raise Exception("key out of index")
                    new_key.append(ele)
            else:
                raise Exception("type of key should be either list or slice")
        else:
            if type(key1) is list:
                new_key = key1[key2]
            elif type(key1) is slice:
                old_start = key1.start if key1.start is not None else 0
                old_step = key1.step if key1.step is not None else 1
                old_stop = key1.stop

                start = key2.start if key2.start is not None else 0
                step = key2.step if key2.step is not None else 1
                stop = key2.stop

                new_start = old_start + old_step * start
                new_step = step * old_step

                if stop is not None:
                    new_stop = old_start + old_step * stop
                else:
                    new_stop = None

                if new_stop is not None and old_stop is not None and new_stop > old_stop:
                    raise Exception("key out of index")
                elif new_stop is None and old_stop is not None:
                    new_stop = old_stop
                else:
                    pass
                new_key = slice(new_start, new_stop, new_step)
            else:
                raise Exception("type of key should be either list or slice")

        return new_key

    def narrow_dependencies_lineage_compress(self):
        pass

    def get_default_execution_platform(self):
        pass

    def compute(self):
        # 拓扑排序
        # 启发式搜索
        # 根据最小二乘拟合的数据来计算时间 + API 读写hdfs的转换时间
        # for id in id_df_map.keys():
        #     print(id, id_df_map[id].func)
        self.orders, self.dag_exist_common_ancestor = get_topological_order(self.id)
        self.lineage_compress()
        self.get_default_execution_platform()
        if self.engine_type is None:
            if self.dag_exist_common_ancestor:
                self.get_best_execution_platforms()
            else:
                self.get_best_execution_platforms_by_dp()

        # 保证读取数据和第一个运算在同一个平台上
        if len(self.orders) > 1:
            optimized_id_platform_map[self.orders[0]] = optimized_id_platform_map[self.orders[1]]

        return self.schedule()

    def get_best_execution_platforms_by_dp(self):
        global optimized_id_platform_map

        ids = self.orders
        dag = id_df_map
        plts = platforms
        k = len(plts)
        n = len(self.orders)
        id_order_map = {}
        import numpy as np
        prev = np.zeros((n, k, 2), dtype=np.int)
        T = np.zeros((n, k), dtype=np.int)
        for i in range(0, n):
            id_order_map[ids[i]] = i
        for i in range(0, n):
            node_id = ids[i]
            for j in range(0, k):
                cur_min_time = 0
                m = 0
                for d in dag[node_id].prev:
                    local_min_time = np.Inf
                    for w in range(0, k):
                        tmp_min_time = T[id_order_map[d]][w] + get_trans_time(d, plts[w], plts[j])
                        if tmp_min_time < local_min_time:
                            local_min_time = tmp_min_time
                            prev[i][j][m] = w
                    cur_min_time = cur_min_time + local_min_time
                    m += 1
                cur_min_time = cur_min_time + get_operator_time(plts[j], node_id)
                T[i][j] = cur_min_time

        last_exe_plt = 0
        global_min_time = T[n - 1][0]
        for j in range(k):
            if T[n - 1][j] < global_min_time:
                last_exe_plt = j
                global_min_time = T[n - 1][j]

        i = n - 1
        id_type_map = dict()
        id_type_map[ids[i]] = last_exe_plt
        import queue
        q = queue.Queue(1000)
        q.put(i)
        while not q.empty():
            i = q.get(block=False)
            node_id = ids[i]
            m = 0
            for d in dag[node_id].prev:
                id_type_map[d] = prev[i][id_type_map[node_id]][m]
                q.put(id_order_map[d])
                m += 1

        for id in id_type_map.keys():
            optimized_id_platform_map[id] = plts[id_type_map[id]]

    def get_best_execution_platforms(self, i=0, total_time=0, trans_cnt=0, tmp_id_platform_map={},
                                     predict_time_list=[]):
        global MAX_TRANS_CNT
        global base_line_time
        global optimized_id_platform_map

        if total_time >= base_line_time:
            return

        if i >= len(self.orders):
            if base_line_time > total_time:
                base_line_time = total_time
                optimized_id_platform_map = tmp_id_platform_map.copy()
            return

        df_id = self.orders[i]
        for p in platforms:
            if id_df_map[df_id].func == "sort_values" and p == "Dask":
                continue
            if id_df_map[df_id].func == "iloc" and p == "Dask":
                continue
            tmp_id_platform_map[df_id] = p
            trans_time = 0
            need_trans = False
            node_time = 0

            if id_df_map[df_id].prev is not None:
                for prev_id in id_df_map[df_id].prev:
                    if prev_id in tmp_id_platform_map and p != tmp_id_platform_map[prev_id]:
                        need_trans = True
                    trans_time += get_trans_time(prev_id, tmp_id_platform_map[prev_id], p)
            if need_trans and trans_cnt + 1 > MAX_TRANS_CNT:
                continue
            else:
                # 从symbolDataFrame 到 时间参数配置
                # 注意第一个节点平台的计算from_csv是直接使用的算完了的大小（初始大小需要设置或预先读），
                # 后面节点使用爸爸们的获得当前计算预估时间
                if id_df_map[df_id].prev is None:
                    node_time = get_read_csv_time(p, id_df_map[df_id].size)
                else:
                    op_time = get_operator_time(p, df_id) + trans_time
                    node_time += op_time

                predict_time_list.append(node_time)
                self.get_best_execution_platforms(i + 1, total_time + node_time,
                                                  trans_cnt + 1 if need_trans else trans_cnt,
                                                  tmp_id_platform_map, predict_time_list)
                tmp_id_platform_map.pop(df_id)
                predict_time_list.pop()


# Todo: 计算不同平台read_csv随着数据规模大小的变化
def get_read_csv_time(p, size):
    return get_operator_time_by_size(p, "read_csv", size)


def get_operator_time(platform, df_id):
    args = calculate_operators_paras(platform, df_id)
    from Octopus.dataframe.predict.getPerformance import get_operator_time_by_size
    return get_operator_time_by_size(platform, id_df_map[df_id].func, *args)


def get_trans_time(prev_df_id, source, target):
    df = id_df_map[prev_df_id]
    size = df.size
    return get_trans_time_by_size(source, target, size[0], size[1])


def calculate_operators_paras(platform, df_id):
    df = id_df_map[df_id]
    if df.func == "from_csv":
        return df.size

    prev = df.prev
    args = []

    if len(prev) == 2:
        args.append(id_df_map[prev[0]].size[0])
        args.append(id_df_map[prev[1]].size[0])
    elif len(prev) == 1:
        args.append(id_df_map[prev[0]].size[0])
        args.append(id_df_map[prev[0]].size[1])
    else:
        raise Exception("len(prev) should either be 2 or 1")

    return args


def get_origin_topological_order(cur, orders=[]):
    orders.append(cur)
    if id_df_map[cur].prev is None:
        return orders

    for prev_id in id_df_map[cur].prev:
        get_origin_topological_order(prev_id, orders)

    return orders


def get_topological_order(id):
    node_refrence_count = {}
    dag_exist_common_ancestor = False
    origin_orders = get_origin_topological_order(id, [])[::-1]
    orders = []
    for id in origin_orders:
        if node_refrence_count.get(id, 0) != 0:
            dag_exist_common_ancestor = True
        else:
            node_refrence_count[id] = True
            orders.append(id)

    return orders, dag_exist_common_ancestor

# path2 = '/Users/jun/workspace/OctDataFrame/tests/dataframe/test_data/marketdata1g.csv'
# df = SymbolDataFrame.from_csv(path=path2).sort_values(by="状态").sum().compute()

# def get_best_execution_platforms_global(id=0, total_time=0, trans_cnt=0):
#     global base_line_time
#     global optimized_id_platform_map
#     global id_platform_map
#     global MAX_TRANS_CNT
#
#     if total_time >= base_line_time:
#         return
#
#     if id > cur_max_id:
#         if base_line_time > total_time:
#             base_line_time = total_time
#             optimized_id_platform_map = id_platform_map
#         return
#
#     for p in platforms:
#         id_platform_map[id] = p
#
#         trans_time = 0
#         need_trans = False
#         for prev_id in id_df_map[id]:
#             if p != id_platform_map[prev_id]:
#                 need_trans = True
#             trans_time += get_trans_time_by_size(prev_id, id)
#         if need_trans and trans_cnt + 1 > MAX_TRANS_CNT:
#             continue
#         else:
#             total_time += get_operator_time_by_size(p, id) + trans_time
#             get_best_execution_platforms(id + 1, total_time, trans_cnt + 1 if need_trans else trans_cnt)
#             id_platform_map.pop(id)

# def predict():
#     orders = get_topological_order()
#     get_best_execution_platforms()
#     for id in orders:
#         id_df_map[id].execute()

# path = "/test_data/target"
# data = [500000*5000]
# df_a = SymbolDataFrame(data, index)
# df_b = df_a.sort_values().iloc[0:200000]
# df_c = df_a.join(df_a)
# df_c.max()

# sdf.show()
