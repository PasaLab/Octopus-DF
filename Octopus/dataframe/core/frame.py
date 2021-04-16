#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
import logging
import numpy as np
import pandas as pd
from pyspark.sql import udf
import pandas.core.common as com
from pyspark.sql.types import StringType
from pandas.core.dtypes.common import is_integer

from Octopus.dataframe.core.utils import derived_from, exe_time, IdGenerator
from Octopus.dataframe.core.sparkinit import spark
from Octopus.dataframe.core.indexManager import IndexManager
from Octopus.dataframe.core.abstractDataFrame import AbstractDataFrame

__all__ = ["Frame", "spark"]


class Frame(AbstractDataFrame):
    def __init__(self, sdf=None, index_col=None, is_cache=False, pdf=None, df_id=None,
                 partition_info=None, index_manager=None, is_df=True,
                 use_secondary_index=False, use_local_index=False):
        AbstractDataFrame.__init__(self, data=sdf)
        self.df_id = IdGenerator.next_id() if df_id is None else df_id  # 每个DataFrame的唯一id

        self.use_pdf = False        # 明确是否使用pandas
        self.big_data_bound = 0     # 大数据的阈值，超过阈值则不使用pandas，否则小数据使用pandas

        self.index_col = index_col  # 索引列

        self.use_secondary_index = use_secondary_index  # 是否使用二级索引
        self.secondary_index_key_in_redis = None   # 二级索引在redis中的key
        self.already_build_secondary_index = False # 是否已经建立二级索引

        self.use_local_index = use_local_index  # 是否使用局部索引
        self.already_build_local_index = False  # 是否已经建立局部索引

        self.pdf = pdf   # 底层的pdf
        self.sdf = None  # 底层的spark dataframe
        self._pdata = None  # spark取最多big_data_bound条数据转化为对应的pandas dataframe

        self.show_num = 200      # 默认使用spark时最多展示的记录条数，太多客户端会内存溢出
        self.record_nums = None  # 总记录条数
        self.ndim = None  # dataframe 维度， 默认2维
        self.columns = None  # dataframe 的列
        self.index = None  # dataframe的索引
        self.meta_data = None    # 轻量级全局索引，包括全局编序区间pdata, 记录数record_nums, 分区号和编序区间下标相互映射关系splitindex_pid_map, pid_splitindex_map
        self.partition_info = partition_info  # 通过直接计算方式获得轻量级全局索引分区元数据

        self.index_manager = index_manager  # dataframe 局部索引管理器

        self.is_df = is_df  # 是否是dataframe，还是series

        self._update(sdf, index_col, is_cache, pdf=pdf)

    def __del__(self):
        self._delete_secondary_index()

    @exe_time
    def _update(self, sdf=None, index_col=None, is_cache=False, pdf=None):
        """
           该类主要来做初始化，由于更新操作也需要，单独抽出来
        """

        self.record_nums = None
        self.meta_data = None
        self.sdf = sdf
        self.index_col = index_col

        if pdf is None:
            if self.sdf is not None and self.meta_data is None:
                self._get_partition_meta_data()
            else:
                raise Exception("sdf and pdf is None")
        else:
            self.use_pdf = True

        if self.use_pdf:
            self._pdata = pdf if pdf is not None else self.to_pandas()
        else:
            if self.index_col and self.sdf:
                if is_cache:
                    self.persist()

                self._pdata = self._to_pdata()  # choose part of data to prevent oom
                if type(self.index_col) is list:
                    self.index_col = self.index_col[0]
                    # 默认只能设置特定列为索引

                if self.index_col in self._pdata.columns:
                    self._pdata = self._pdata.set_index(self.index_col).drop(columns='index', errors='ignore')

            else:
                self._pdata = self._to_pdata()

            if self._pdata.index.name == "index":
                self._pdata.index.name = ""

        self.ndim = None if not isinstance(self._pdata, (pd.Series, pd.DataFrame)) else self._pdata.ndim
        self.columns = None if not isinstance(self._pdata, pd.DataFrame) else self._pdata.columns
        self.index = None if not isinstance(self._pdata, pd.DataFrame) else self._pdata.index

        if self.use_secondary_index:
            self._build_secondary_index(self.index_col)

        if self.use_local_index:
            self._build_local_index()

    def _build_secondary_index(self, index_col):
        if self.index_col == index_col and self.already_build_secondary_index:
            return

        df_id = self.df_id
        pdata, record_nums, splitindex_pid_map, pid_splitindex_map = self.meta_data
        self.secondary_index_key_in_redis = df_id + '_' + str(index_col)
        secondary_index_key_in_redis = self.secondary_index_key_in_redis
        logging.info("Build secondary index {key}".format(key=secondary_index_key_in_redis))

        offset_map = {}
        for split_index in splitindex_pid_map.keys():
            offset_map[split_index] = splitindex_pid_map[split_index]

        def f(split_index, iterator):
            if split_index not in offset_map.keys():
                return []

            offset = 0
            try:
                from redis import Redis
                #  重构时统一抽取配置Redis
                r = Redis(host='simple27', port=6379, decode_responses=True)
            except Exception as exp:
                logging.error("write meta_data of dataframe to redis failed")

            start_offset = offset_map[split_index]
            try:
                while iterator:
                    record = next(iterator)
                    key = record[index_col]
                    if r.hexists(secondary_index_key_in_redis, key):
                        r.hmset(secondary_index_key_in_redis, {key: -1})
                    else:
                        r.hmset(secondary_index_key_in_redis, {key: start_offset + offset})
                    offset += 1
            except StopIteration as exp:
                return []
            # return []

        self.sdf.rdd.mapPartitionsWithIndex(f).collect()
        self.already_build_secondary_index = True

    def _delete_secondary_index(self):
        if self.secondary_index_key_in_redis is None:
            return
        try:
            logging.info("Delete secondary_index {key}".format(key=self.secondary_index_key_in_redis))
            from redis import Redis
            # 重构时统一抽取配置Redis
            r = Redis(host='simple27', port=6379, decode_responses=True)
            r.delete(self.secondary_index_key_in_redis)
        except Exception as exp:
            logging.error("write meta_data of dataframe {} to redis failed")
            logging.error(exp)

    def _build_local_index(self):
        if self.index_col is None:
            raise Exception('index_col is None')

        if self.already_build_local_index:
            return

        self.index_manager = IndexManager(self.sdf, self.df_id)
        self.index_manager.create_index(self.index_col)
        self.already_build_local_index = True

    @exe_time
    def persist(self):
        self.sdf = self.sdf.cache()
        self.sdf.count()

    def _get_partition_meta_data_from_redis(self):
        from redis import Redis
        r = Redis(host="simple27", port=6379, decode_responses=True)
        partition_info = r.hgetall(name="df" + str(self.df_id))

        return list(partition_info.items())

    @exe_time
    def _get_partition_meta_data(self):  # 可以优化为由前一个rdd 的 partition 信息得到
        if self.partition_info is None:
            self.partition_info = self._get_partition_meta_data_from_redis()
            if self.partition_info == []:
                def f(split_index, iterator):
                    count = len(list(iterator))
                    if count != 0:
                        yield (split_index, count)

                self.partition_info = self.sdf.rdd.mapPartitionsWithIndex(f).collect()

        self.partition_info.sort()
        record_nums = 0
        partitions = []
        splitindex_pid_map = {}
        pid_splitindex_map = {}
        pid = 0

        for ele in self.partition_info:
            partitions.append((record_nums, record_nums + int(ele[1])))
            splitindex_pid_map[ele[0]] = pid
            pid_splitindex_map[pid] = ele[0]
            record_nums += int(ele[1])
            pid += 1

        self.record_nums = record_nums
        self.meta_data = partitions, record_nums, splitindex_pid_map, pid_splitindex_map
        self.use_pdf = self.record_nums <= self.big_data_bound

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

        if self.record_nums is None:
            self._get_partition_meta_data()

        return self.record_nums, len(self.columns)

    @classmethod
    def _type_filter(cls, dtype):
        if dtype == 'bool':
            return 'boolean'
        elif dtype in ['int', 'longlong', 'int64']:
            return 'long'
        elif dtype == 'str':
            return 'string'
        elif dtype == 'float':
            return 'double'
        else:
            return dtype

    def astype(self, dtype, copy=True, errors='raise', **kwargs):
        """
        Cast a pandas object to a specified dtype dtype.

        Parameters
        ----------
        dtype : Use a numpy.dtype or Python type to cast entire pandas object to the same type. Alternatively,
            use {col: dtype, ...}, where col is a column label and dtype is a numpy.dtype or Python type to cast one or
            more of the SparkDataFrame’s columns to column-specific types.
        copy : Not support.
        errors : Not support.
        """

        if self.use_pdf:
            return self._pdata.astype(dtype, copy=copy, errors=errors, kwargs=kwargs)

        sdf = self.sdf
        if not isinstance(dtype, dict):
            for col in self.columns:
                sdf = sdf.withColumn(col, sdf[col].cast(self._type_filter(dtype)))
        else:
            for col in dtype.keys():
                if col in self.columns:
                    sdf = sdf.withColumn(col, sdf[col].cast(self._type_filter(dtype[col])))
                else:
                    raise Exception("Column is not in dataframe columns.")
        return type(self)(sdf, self.index_col)

    def __getitem__(self, key):
        from pyspark.sql.column import Column
        from Octopus.dataframe.core.sparkSeries import Series
        from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
        if self.use_pdf:
            if isinstance(key, Series):
                key = key._pdata
            pdata = self._pdata.__getitem__(key)
            if isinstance(pdata, pd.Series):
                return Series(pdf=pdata)
            elif isinstance(pdata, pd.DataFrame):
                return SparkDataFrame(pdf=pdata)
            else:
                return pdata

        key = com.apply_if_callable(key, self)
        try:
            if key in self.columns:
                return self._getitem_column(key)
            elif isinstance(key, Column):
                return SparkDataFrame(self.sdf[key], is_cache=False)
        except Exception as exp:
            pass

        if isinstance(key, slice):
            return self._getitem_slice(key)

        if isinstance(key, (np.ndarray, list, Series)):
            return self._getitem_array(key)
        elif isinstance(key, SparkDataFrame):
            return self._getitem_frame(key)
        else:
            return self._getitem_column(key)

    def _getitem_column(self, key):
        if isinstance(key, str):
            from Octopus.dataframe.core.sparkSeries import Series
            return Series(sdf=self.sdf.select(key), index_col=self.index_col)
        if is_integer(key):
            return self.iloc[key]
        else:
            raise KeyError("Can only get one value each time by index.")

    def _getitem_slice(self, key):
        if is_integer(key.start) or is_integer(key.stop):
            return self.iloc[key]
        else:
            return self.loc[key]

    def _getitem_array(self, key):
        from Octopus.dataframe.core.sparkSeries import Series
        from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
        from Octopus.dataframe.core.methods import is_integer_array
        if isinstance(key, Series):
            return SparkDataFrame(self.sdf[key.col], index_col=self.index_col)
        if isinstance(key, np.ndarray):
            key = key.tolist()
        if com.is_bool_indexer(key):
            raise Exception("Not support bool index.")

        elif is_integer_array(key):
            return self.iloc[key, :]
        else:
            return self.loc[:, key]

    def _getitem_frame(self, key):
        raise Exception("Not Support yet")

    @exe_time
    def __setitem__(self, key, value):
        if self.use_pdf:
            return self._pdata.__setitem__(key, value)

        from Octopus.dataframe.core.sparkSeries import Series
        from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame

        key = com.apply_if_callable(key, self)
        value = com.apply_if_callable(value, self)

        if isinstance(value, (np.ndarray, Series)):
            value = value.tolist()
        if isinstance(key, slice):
            return self._setitem_slice(key, value)
        elif isinstance(key, (np.ndarray, list, Series)):
            return self._setitem_array(key, value)
        elif isinstance(key, SparkDataFrame):
            return self._setitem_frame(key, value)
        else:
            return self._setitem_column(key, value)

    @exe_time
    @derived_from(pd.DataFrame)
    def _setitem_slice(self, key, value):
        if is_integer(key.start) or is_integer(key.stop):
            self.iloc[key] = value
        else:
            self.loc[key] = value

    @exe_time
    @derived_from(pd.DataFrame)
    def _setitem_array(self, key, value):
        from Octopus.dataframe.core.sparkSeries import Series
        from Octopus.dataframe.core.methods import is_integer_array
        if isinstance(key, (Series, np.ndarray)):
            key = key.tolist()
        if com.is_bool_indexer(key):
            raise Exception("Not support bool index.")
        elif isinstance(self, Series) and is_integer_array(key):
            self.iloc[key] = value
        else:
            self.loc[:, key] = value

    @exe_time
    @derived_from(pd.DataFrame)
    def _setitem_frame(self, key, value):
        raise NotImplementedError("Not support yet.")

    @exe_time
    def _setitem_column(self, key, value):
        from Octopus.dataframe.core.sparkSeries import Series
        if isinstance(key, str):
            self.loc[:, key] = value
        elif isinstance(self, Series) and is_integer(key):
            self.iloc[key] = value
        else:
            raise KeyError("Can only set one column each time.")

    def _get_columns(self, key):
        return self.sdf.select(key + [self.index_col])

    @property
    @derived_from(pd.DataFrame)
    def loc(self):
        if not self.use_pdf:
            if self.meta_data is None:
                self._get_partition_meta_data()
        from .indexing import _LocIndexer
        return _LocIndexer(self)

    @property
    @derived_from(pd.DataFrame)
    def iloc(self):
        if not self.use_pdf:
            if self.meta_data is None:
                self._get_partition_meta_data()
        from .indexing import _iLocIndexer
        return _iLocIndexer(self)

    @derived_from(pd.DataFrame)
    def drop(self, labels=None, axis=0, index=None, columns=None, level=None,
             inplace=False, errors='raise'):
        pass

    @derived_from(pd.DataFrame)
    def _drop_axis(self, sdf, labels, axis, level=None, errors='raise'):
        """
        Notes:
        Octopus does not support arguments of 'level' and 'errors' now.
        """
        warnings.warn("Does not support level and errors now.")

        if axis == 0:
            return self._drop_rows(sdf, labels)
        elif axis == 1:
            return self._drop_cols(sdf, labels)
        else:
            raise KeyError("Octopus only supports 2 dimensions now.")

    @classmethod
    def _drop_cols(cls, sdf, labels):
        if not isinstance(labels, list):
            labels = [labels]
        for column in labels:
            sdf = sdf.drop(column)
        return sdf

    def _drop_rows(self, sdf, labels):
        if not isinstance(labels, list):
            labels = [labels]

        return sdf.where(sdf[self.index_col].isin(labels) == False)

    def _add_rows(self, sdf, data, indexers=None):
        data = self._has_valid_value(len(indexers), len(self.columns), data)
        data = data[:]
        length = len(data)
        for i in range(length):
            data[i] = data[i] + [indexers[i]]

        pdf = pd.DataFrame(data, columns=sdf.columns)
        return sdf.union(spark.createDataFrame(pdf))

    @exe_time
    def _update_cols(self, sdf, cols, data, ret_sdf=False):
        data = self._has_valid_value(len(self.index.tolist()), len(cols), data)
        index_col = self.index_map["#uniq#"].tolist()
        data = np.array(data).transpose().tolist()

        for col in cols:
            def update_col(value):
                return str(data[cols.index(col)][index_col.index(value)])

            update_col_udf = udf(update_col, StringType())
            sdf = sdf.withColumn(col, update_col_udf(sdf["#uniq#"]))
        if ret_sdf:
            return sdf
        else:
            self._update(sdf.drop("#uniq#"), self.index_col)


    @classmethod
    def _has_valid_value(cls, indexers_num, columns_num, data):
        from Octopus.dataframe.core.methods import is_single
        if is_single(data):
            data = np.full((indexers_num, columns_num), data).tolist()
            return data
        if len(data) != indexers_num:
            raise ValueError("Values' shape is not consistent with keys' shape.")

        for row in data:
            if len(row) != columns_num:
                raise ValueError("Values' shape is not consistent with keys' shape.")
        return data

    @derived_from(pd.DataFrame)
    def rename(self, *args, **kwargs):

        if self.use_pdf:
            from Octopus.dataframe.core import SparkDataFrame
            return SparkDataFrame(pdf=self._pdata.rename(*args, **kwargs))

        columns = kwargs.pop('columns', None)
        inplace = kwargs.pop('inplace', False)
        sdf = self.sdf
        if isinstance(columns, dict):
            for col in columns:
                if col in sdf.columns:
                    sdf = sdf.withColumnRenamed(col, columns[col])
                else:
                    raise KeyError("Col {} is not SparkDataFrame's columns.".format(col))
        else:
            raise KeyError("The parameter of columns should be a dict.")
        if inplace:
            self._update(sdf, self.index_col, is_cache=True)
        else:
            from Octopus.dataframe.core import SparkDataFrame
            return SparkDataFrame(sdf, self.index_col, is_cache=True)

    @derived_from(pd.DataFrame)
    def to_csv(self, path_or_buf=None, sep=",", na_rep='', float_format=None, columns=None, header=True, index=True,
               index_label=None, mode='w', encoding=None, compression=None, quoting=None, quotechar='"',
               line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        """
        Write Series to a comma-separated values (csv) file
        Parameters
        ----------
        path_or_buf : string or file handle, default None
            File path or object, if None is provided the result is returned as
            a string.
        sep : character, default ","
            Field delimiter for the output file.
        na_rep : string, default ''
            Missing data representation
        float_format : Not support.
        header : boolean, default False
            Write out series name
        index : Not support.
        index_label : Not support.
        mode : Not support.
        encoding : Not support.
        compression : string, optional
            A string representing the compression to use in the output file.
            Allowed values are 'gzip', 'bz2', 'zip', 'xz'. This input is only
            used when the first argument is a filename.
        quoting : Not support.
        date_format: string, default None
            Format string for datetime objects.
        doublequote : Not support.
        escapechar : Not support.
        decimal: Not support.
        """
        if self.use_pdf:
            self.sdf = spark.createDataFrame(self._pdata)

        self.sdf.write.csv(path_or_buf, sep=sep, compression=compression, nullValue=na_rep, header=header,
                           quote=quotechar, dateFormat=date_format, escape=escapechar, mode="overwrite")

    @exe_time
    def _to_pdata(self):
        """Write a Octopus SparkDataFrame to a Pandas SparkDataFrame"""
        pdf = pd.DataFrame.from_records(self.sdf.take(self.show_num), columns=self.sdf.columns)
        return pdf

    @exe_time
    def to_pandas(self):
        """Write a Octopus SparkDataFrame to a Pandas SparkDataFrame"""
        if self.use_pdf and self._pdata is not None:
            return self._pdata

        return self.sdf.toPandas()

    def to_json(self, path_or_buf=None, orient=None, date_format=None,
                double_precision=10, force_ascii=True, date_unit='ms',
                default_handler=None, lines=False, compression=None):
        """
        Octopus Just support path,compression
        path_or_buf : path of the source file
        Compression : none, bzip2, gzip, lz4, snappy and deflate
        """
        if self.use_pdf:
            self.sdf = spark.createDataFrame(self._pdata)

        self.sdf.write.json(path_or_buf, compression=compression)

    @derived_from(pd.DataFrame)
    def to_records(self, index=True, convert_datetime64=True):
        if self.use_pdf:
            return self._pdata.to_records(index=index, convert_datetime64=convert_datetime64)

        rec_list = []
        for row in self.sdf.collect():
            row_list = []
            for col in self.columns:
                row_list.append(row[col])
            rec_list.append(tuple(row_list))
        names = ','.join(self.columns)
        rec_array = np.core.records.fromrecords(rec_list, names=names)
        return rec_array

    def to_text(self, path, compression=None):
        self.sdf.write.text(path, compression)

    def __str__(self):
        return self._pdata.__str__()

    def __iter__(self):
        return self._pdata.__iter__()

    # @exe_time
    def _add_id_column(self):
        pdata, num, splitindex_pid_map, pid_splitindex_map = self.meta_data

        def f(splitindex, iterator):
            if splitindex not in splitindex_pid_map.keys():
                return

            def update(origin_data, id):
                new_data = origin_data.asDict()
                new_data["index"] = id
                from pyspark.sql.types import Row
                res = Row(*new_data)
                res = res(*new_data.values())
                return res

            offset = 0
            while iterator:
                record = next(iterator)
                yield update(record, pdata[splitindex_pid_map[splitindex]][0] + offset)
                offset += 1

        # self.sdf = self.sdf.rdd.mapPartitionsWithIndex(f).toDF()
        self.sdf = spark.createDataFrame(self.sdf.rdd.mapPartitionsWithIndex(f), samplingRatio=1)
        self.index_col = "index"


@exe_time
def to_pandas(sdf, index_col):
    if index_col is not None:
        return pd.DataFrame.from_records(sdf.collect(), columns=sdf.columns).set_index(index_col)
    return pd.DataFrame.from_records(sdf.collect(), columns=sdf.columns)

