#!/usr/bin/env python
#  -*- coding: utf-8 -*-

import logging
import numpy as np
import pandas as pd
import pandas.core.common as com
from pandas.core.dtypes.common import is_integer

from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
from Octopus.dataframe.core.sparkSeries import Series
from Octopus.dataframe.core.utils import exe_time
from Octopus.dataframe.core.sparkinit import spark
from Octopus.dataframe.core.methods import (
    getitem_axis,
    get_df_or_series,
    is_single,
    is_empty_slice
)


class LocationIndexer(object):
    def __init__(self, obj):
        self.obj = obj
        self.sdf = self.obj.sdf

    def __getitem__(self, key):
        if self.obj.use_pdf:
            pdata = None
            if isinstance(self, _LocIndexer):
                pdata = self.obj._pdata.loc[key]
            elif isinstance(self, _iLocIndexer):
                pdata = self.obj._pdata.iloc[key]
            else:
                raise Exception("operation is not loc or iloc")

            if isinstance(pdata, pd.Series):
                return Series(pdf=pdata)
            elif isinstance(pdata, pd.DataFrame):
                return SparkDataFrame(pdf=pdata)
            else:
                return pdata

        iindexer, cindexer = self._key2indexer(key)
        return self._loc(iindexer, cindexer)


    @exe_time
    def _calc_record_partition_id_offset_by_binary_search(self, row_id):
        pdata, item_num, splitindex_pid_map, pid_splitindex_map = self.obj.meta_data
        pnum = len(pdata)
        if row_id == pdata[-1][1]:
            return pnum - 1, pdata[pnum - 1][1] - pdata[pnum - 1][0] + 1
        if row_id > pdata[-1][1] or row_id < 0:
            raise KeyError("location index out of range")

        left = 0
        right = pnum
        while left < right:
            mid = left + (right - left) / 2
            if row_id in range(pdata[mid][0], pdata[mid][1]):
                return pid_splitindex_map[mid], row_id - pdata[mid][0] + 1
            elif row_id >= pdata[mid][1]:
                left = mid
            elif row_id < pdata[mid][0]:
                right = mid

        return pid_splitindex_map[left], row_id - pdata[left][0] + 1


    @exe_time
    def _calc_record_partition_id_offset_by_interval_estimation(self, row_id):
        pdata, item_num, splitindex_pid_map, pid_splitindex_map = self.obj.meta_data
        pnum = len(pdata)
        if row_id == pdata[-1][1]:
            return pid_splitindex_map[pnum - 1], pdata[pnum - 1][1] - pdata[pnum - 1][0] + 1

        if row_id > pdata[-1][1] or row_id < 0:
            raise KeyError("location index out of range")

        import math
        id_estimate = int(row_id / math.ceil(item_num / pnum))

        while 0 <= id_estimate < pnum:
            if row_id in range(pdata[id_estimate][0], pdata[id_estimate][1]):
                return pid_splitindex_map[id_estimate], row_id - pdata[id_estimate][0]
            elif row_id >= pdata[id_estimate][1]:
                id_estimate += 1
            elif row_id < pdata[id_estimate][0]:
                id_estimate -= 1

        raise Exception("Cannot find index of {}'s data".format(row_id))

    @classmethod
    def _calc_variance(cls, pdata):
        import numpy as np
        arr = list(map(lambda x: x[1] - x[0], pdata))
        return np.var(arr)

    @exe_time
    def _calc_record_partition_id_offset(self, row_id, theta=100):
        pdata, item_num, splitindex_pid_map, pid_splitindex_map = self.obj.meta_data
        v = self._calc_variance(pdata)
        if v < theta:
            return self._calc_record_partition_id_offset_by_interval_estimation(row_id)
        else:
            return self._calc_record_partition_id_offset_by_binary_search(row_id)

    @classmethod
    def _calc_new_mapping(cls, old_mapping, operator):
        if type(operator) is not list and type(operator) is not slice:
            raise Exception("type operator should be either list or slice")

        if type(operator) is list:
            new_mapping = []
            if type(old_mapping) is list:
                for i in operator:
                    if i >= len(old_mapping):
                        raise Exception("key out of index")
                    new_mapping.append(old_mapping[i])
            elif type(old_mapping) is slice:
                start = slice.start
                step = slice.step
                stop = slice.stop
                for i in operator:
                    ele = start + i * step
                    if ele >= stop:
                        raise Exception("key out of index")
                    new_mapping.append(ele)
            else:
                raise Exception("type old mapping should be either list or slice")
        else:
            if type(old_mapping) is list:
                new_mapping = old_mapping[operator]
            elif type(old_mapping) is slice:
                old_start = old_mapping.start
                old_step = old_mapping.step
                old_stop = old_mapping.stop

                start = operator.start
                step = operator.step
                stop = operator.stop

                new_start = old_start + old_step * start
                new_step = step * old_step
                new_stop = old_start + old_step * stop
                if new_stop > old_stop:
                    raise Exception("key out of index")

                new_mapping = slice(new_start, new_stop, new_step)

            else:
                raise Exception("type old_mapping should be either list or slice")

        return new_mapping

    def _get_records_slice(self, row_ids):
        if not isinstance(row_ids, slice):
            raise Exception("row_ids should be instance of slice")

        start = 0 if row_ids.start is None else row_ids.start
        stop = self.obj.record_nums if row_ids.stop is None else row_ids.stop
        step = 1 if row_ids.step is None else row_ids.step

        if (start is None or start == 0) \
                and (stop is None or stop >= self.obj.shape[0]) \
                and (step is None or step is 1):
            return self.sdf

        if step is None:
            step = 1

        row_infos, row_info_map = self._calc_slice_partition_id_offset(start, stop, step)
        new_df_partition_info = self._calc_new_df_partition_info_slice(row_infos, row_info_map)
        # new_df_partition_info = None

        def f2(split_index, iterator):
            while iterator:
                data = next(iterator)
                yield data["value"]

        # 是否使用局部索引的判断
        if self.obj.index_manager is not None:
            def f3(split_index, iter):
                if row_info_map.get(split_index, -1) == -1:
                    return []

                # import time
                # t0 = time.time()
                object_id = next(iter)

                # arrow 目前只能在linux上进行读写使用，mac和windows无法使用
                from pyarrow import plasma
                import pyarrow as pa
                client = plasma.connect("/tmp/plasma", "", 0)

                # Fetch the Plasma object
                if not client.contains(object_id):
                    raise Exception("The node does not contain object_id of ", str(object_id))

                [data] = client.get_buffers([object_id])  # Get PlasmaBuffer from ObjectID
                buffer = pa.BufferReader(data)

                # Convert object back into an Arrow RecordBatch
                reader = pa.RecordBatchStreamReader(buffer)
                record_batch = reader.read_all()

                # Convert back into Pandas
                pdf = record_batch.to_pandas()

                info_index = row_info_map[split_index]
                if split_index == row_infos[-1][0]:
                    num = row_infos[-1][4]
                else:
                    num = row_infos[info_index][3]

                if split_index in range(row_infos[0][0] + 1, row_infos[-1][0]) and row_infos[info_index][2] == 1:
                    res = pdf
                else:
                    if split_index != row_infos[-1][0]:
                        res = pdf.iloc[int(row_infos[info_index][1])::step]
                    else:
                        res = pdf.iloc[int(row_infos[info_index][1]):int(row_infos[-1][3]):step]

                    if step < 0:
                        from pyspark.sql.types import Row
                        res = list(map(lambda row, offset: Row(key=-1 * (num + offset), value=row), res,
                                       range(0, len(res))))

                from pyspark.sql.types import Row

                def to_row(row_data):
                    row_data = row_data.to_dict()
                    row = Row(*row_data)
                    row = row(*row_data.values())
                    return row

                res = list(res.apply(to_row, axis=1))

                return res

            if step < 0:
                return self.obj.index_manager.index_rdd.mapPartitionsWithIndex(f3).toDF().sort(
                    "key").rdd.mapPartitionsWithIndex(f2).toDF(), new_df_partition_info
            else:
                from Octopus.dataframe.core.sparkinit import spark
                sdf_by_index = self.obj.index_manager.index_rdd.mapPartitionsWithIndex(f3).toDF()
                return sdf_by_index, new_df_partition_info

        else:
            if row_infos is None or (step > 0 and start >= stop):
                from Octopus.dataframe.core.sparkinit import spark
                return spark.createDataFrame([], self.obj.sdf.schema), None

            def f(split_index, iterator):
                if row_info_map.get(split_index, -1) == -1:
                    return

                info_index = row_info_map[split_index]
                if split_index == row_infos[-1][0]:
                    num = row_infos[-1][4]
                else:
                    num = row_infos[info_index][3]

                if split_index in range(row_infos[0][0] + 1, row_infos[-1][0]) and row_infos[info_index][2] == 1:
                    while iterator:
                        yield next(iterator)
                else:
                    offset = 0
                    while offset < (row_infos[info_index][1]):
                        next(iterator)
                        offset += 1

                    while iterator:
                        # step < 0 should reverse the data
                        if split_index == row_infos[-1][0] and offset + 1 >= row_infos[-1][3]:
                            raise StopIteration

                        if step < 0:
                            data = next(iterator)
                            from pyspark.sql.types import Row
                            row = Row(key=-1 * (num + offset), value=data)
                            yield row
                        else:
                            yield next(iterator)

                        offset += 1
                        for i in range(0, abs(step) - 1):
                            next(iterator)
                            offset += 1

            if step < 0:
                return self.obj.sdf.rdd.mapPartitionsWithIndex(f).toDF().sort(
                    "key").rdd.mapPartitionsWithIndex(f2).toDF(), new_df_partition_info
            else:
                sdf = self.obj.sdf.rdd.mapPartitionsWithIndex(f).toDF()
                return sdf, new_df_partition_info

    @exe_time
    def _calc_partition_row_num(self, partition_info, row_info, last_split_index_id):
        start = partition_info[0]
        end = partition_info[1]
        step = row_info[2]
        if row_info[0] == last_split_index_id:
            end = min(row_info[3], end)
        if step < 0:
            raise Exception("step should be greater than 0 ")
        p_num = (end - 1 - start) // step + 1

        return row_info[0], p_num

    # 直接计算出元数据替代遍历RDD获取元数据的方式
    @exe_time
    def _calc_new_df_partition_info_slice(self, row_infos, row_info_map):
        partitions, id_num, splitindex_pid_map, pid_splitindex_map = self.obj.meta_data
        new_partitions_info = []
        last_split_index_id = pid_splitindex_map[len(partitions) - 1]
        for i in range(0, len(row_infos)):
            new_partition_info = self._calc_partition_row_num(partitions[splitindex_pid_map[row_infos[i][0]]],
                                                               row_infos[i],
                                                               last_split_index_id)
            new_partitions_info.append(new_partition_info)

        return new_partitions_info

    @exe_time
    def _calc_slice_partition_id_offset(self, start, stop, step):
        if (start < stop and step < 0) or (start > stop and step > 0):
            return None
        pdata, item_num, splitindex_pid_map, pid_splitindex_map = self.obj.meta_data

        if start < 0 and stop < 0:
            start += item_num
            stop += item_num

        if step < 0:
            stop += abs(step) if (start - stop) % abs(step) == 0 else (start - stop) % abs(step)
            start, stop = stop, start + 1

        start_partition, start_offset = self._calc_record_partition_id_offset(start)
        stop_partition, stop_offset = self._calc_record_partition_id_offset(stop)

        row_info = [(start_partition, start_offset, step, pdata[splitindex_pid_map[start_partition]][0])]
        row_info_map = dict()
        row_info_map[start_partition] = len(row_info) - 1
        if start_partition == stop_partition:
            row_info = [
                (start_partition, start_offset, step, stop_offset, pdata[splitindex_pid_map[start_partition]][0])]
            row_info_map[start_partition] = len(row_info) - 1
        else:
            for i in range(splitindex_pid_map[start_partition] + 1, splitindex_pid_map[stop_partition] + 1):
                # 每个partition的开始偏移
                start_offset = (abs(step) - (pdata[i][0] - start) % abs(step)) % abs(step)
                if pid_splitindex_map[i] != stop_partition:
                    row_info.append((pid_splitindex_map[i], start_offset, step, pdata[i][0]))
                else:
                    row_info.append((pid_splitindex_map[i], start_offset, step, stop_offset, pdata[i][0]))
                row_info_map[pid_splitindex_map[i]] = len(row_info) - 1

        return row_info, row_info_map

    @exe_time
    def _key2indexer(self, key):
        if isinstance(key, tuple):
            key = tuple(com.apply_if_callable(x, self.obj) for x in key)
            if len(key) > self.obj.ndim:
                msg = 'Too many location indexers.'
                raise pd.core.indexing.IndexingError(msg)

            iindexer = key[0]
            cindexer = key[1]
            if isinstance(self.obj, Series):
                single_col = len(self.obj.columns) == 1
                if single_col:
                    iindexer = key[0] if not isinstance(key[0], slice) else key[1]
                    cindexer = None
                else:
                    iindexer = None
                    cindexer = key[0] if not isinstance(key[0], slice) else key[1]
        else:
            key = com.apply_if_callable(key, self.obj)
            if isinstance(self.obj, SparkDataFrame):
                iindexer = key
                cindexer = None
            elif isinstance(self.obj, Series):
                single_col = len(self.obj.columns) == 1
                if single_col:
                    iindexer = key
                    cindexer = None
                else:
                    iindexer = None
                    cindexer = key

        return iindexer, cindexer

    def __setitem__(self, key, value):
        logging.debug("enter spark setitem")
        if self.obj.use_pdf:
            pdata = None
            if isinstance(self, _LocIndexer):
                self.obj._pdata.loc[key] = value
            elif isinstance(self, _iLocIndexer):
                self.obj._pdata.iloc[key] = value
            else:
                raise Exception("operation is not loc or iloc")
            return

        iindexer, cindexer = self._key2indexer(key)
        value = com.apply_if_callable(value, self.obj)
        if isinstance(value, (np.ndarray, Series)):
            value = value.tolist()

        self._set(iindexer, cindexer, value)

    def _set(self, iindexer, cindexer, value):
        pass

    @exe_time
    def _calc_slice_partition_value(self, row_ids, columns, values):
        if not isinstance(row_ids, slice):
            raise Exception("Not instance of slice")
        start, stop, step = row_ids.start, row_ids.stop, row_ids.step
        if step is None:
            step = 1

        if start is None:
            start = 0

        if stop is None:
            stop = self.obj.shape[0]

        row_ids = list(range(start, stop, step))
        return self.calc_list_partition_value(row_ids, columns, values)

    @exe_time
    def _set_record_slice(self, row_ids, columns, values):
        if not isinstance(row_ids, slice):
            raise Exception("Not instance of slice")
        start, stop, step = row_ids.start, row_ids.stop, row_ids.step
        if step is None:
            step = 1

        if start is None:
            start = 0

        if stop is None:
            stop = self.obj.shape[0]

        # 得到的row_infos 都是由start到stop，step为绝对值，如果step为负，为了恢复原顺序，得到的数据需倒置
        row_infos, row_info_map = self._calc_slice_partition_id_offset(start, stop, step)
        value_infos = self._calc_slice_partition_value(row_ids, columns, values)
        if row_infos is None:
            from Octopus.dataframe.core.sparkinit import spark
            return spark.createDataFrame([], self.obj.sdf.schema)

        def update(origin_data, row_id):
            value = []
            if isinstance(value_infos, list):
                value = value_infos
            elif isinstance(value_infos, dict):
                value = value_infos[row_id]

            if len(columns) != len(value):
                raise Exception("Update column numbers not equal to value_info")
            new_data = origin_data.asDict()
            for i in range(len(columns)):
                new_data[columns[i]] = value[i]
            from pyspark.sql.types import Row
            res = Row(*new_data)
            res = res(*new_data.values())
            return res

        def f(split_index, iterator):

            if split_index not in range(row_infos[0][0], row_infos[-1][0] + 1):
                while iterator:
                    yield next(iterator)

            info_index = row_info_map[split_index]
            if split_index == row_infos[-1][0]:
                num = row_infos[-1][4]
            else:
                num = row_infos[info_index][3]

            offset = 0
            if split_index in range(row_infos[0][0] + 1, row_infos[-1][0]) and row_infos[info_index][2] == 1:
                while iterator:
                    record = next(iterator)
                    yield update(record, num + offset)
                    offset += 1

            else:
                while offset < row_infos[info_index][1]:
                    yield next(iterator)
                    offset += 1

                while iterator:
                    if split_index == row_infos[-1][0] and offset + 1 >= row_infos[-1][3]:
                        break
                    record = next(iterator)
                    yield update(record, num + offset)
                    offset += 1

                    for i in range(0, abs(step) - 1):
                        yield next(iterator)
                        offset += 1

                while iterator:
                    yield next(iterator)

        return self.obj.sdf.rdd.mapPartitionsWithIndex(f).toDF()

    @exe_time
    def _set_record_single(self, row_id, key_vals):
        partition, offset = self._calc_record_partition_id_offset_by_interval_estimation(row_id=row_id)
        row_infos = [(partition, offset, key_vals)]
        step = 1

        def f(split_index, iterator):
            if split_index != row_infos[0][0]:
                while iterator:
                    yield next(iterator)
            else:
                offset = 0
                while offset < (row_infos[split_index][1] - 1):
                    yield next(iterator)
                    offset += 1
                data = next(iterator)
                data = data.asDict()

                for key in key_vals:
                    data[key] = key_vals[key]

                from pyspark.sql.types import Row

                res = Row(*data)
                res = res(*data.values())
                yield res
                while iterator:
                    yield next(iterator)

        return self.obj.sdf.rdd.mapPartitionsWithIndex(f).toDF()

    @exe_time
    def _bool2keys(self, key, axis=1):
        if axis == 0:
            raise Exception("Not support.")
        obj_index = self._get_index_axis(axis=0) if axis == 0 else self.obj.columns
        if com.is_bool_indexer(key):
            if isinstance(key, Series) and not key.index.equals(obj_index):
                logging.warning("Boolean Series key will be reindexed to match "
                              "SparkDataFrame index.")
            elif len(key) != len(obj_index):
                raise ValueError('Item wrong length %d instead of %d.' %
                                 (len(key), len(obj_index)))
            else:
                indexer = []
                for item, i in zip(obj_index, key):
                    if i:
                        indexer.append(item)
                return indexer
        return key

    def _loc(self, iindexer, cindexer):
        pass

    @property
    def _name(self):
        return self.obj._name

    @exe_time
    def _get_index_axis(self, axis):
        if axis == 0:
            raise Exception("Not support index now.")
        elif axis == 1:
            df_index = self.obj.columns.tolist()
        else:
            raise NotImplementedError("Octopus only supports 2 dimensions now.")
        return df_index

    @exe_time
    def _get_indexer_axis(self, axis):
        df_indexer = None
        if axis == 0:
            df_indexer = self.obj.index
        elif axis == 1:
            df_indexer = self.obj.columns
        else:
            raise NotImplementedError("Octopus only supports 2 dimensions now.")
        return df_indexer

    @exe_time
    def _check_integer_valid(self, loc, axis=0):
        df_index = self._get_index_axis(axis)
        if loc > len(df_index) or loc < 0:
            raise KeyError("Key %s is Out of index bounds" % loc)

    @exe_time
    def _get_pos_axis(self, indexer, axis=1, start=True):
        if axis == 0:
            index_list = self.obj.index.tolist()

        elif axis == 1:
            index_list = self.obj.columns.tolist()
        else:
            raise KeyError("Only Support 2 dimensions now.")

        if indexer is None:
            return 0 if start else len(index_list) - 1
        else:
            if index_list.count(indexer) != 1:
                raise KeyError("Cannot get right slice bound for non-unique label: '{}'".format(indexer))
            return index_list.index(indexer)

    @exe_time
    def _label_index2keys(self, indexer, axis):
        if isinstance(indexer, slice):
            keys = self._label_slice2keys(indexer, axis)
        elif isinstance(indexer, (list, np.ndarray, Series)):
            if isinstance(indexer, (np.ndarray, Series)):
                indexer = indexer.tolist()
            if com.is_bool_indexer(indexer):
                indexer = self._bool2keys(indexer, axis)
            keys = indexer
        elif is_single(indexer):
            keys = [indexer]
        elif indexer is None:
            return self._get_index_axis(axis)
        else:
            raise KeyError("Method loc only support slice, list and single key now!")

        return keys

    @exe_time
    def _label_slice2keys(self, indexer, axis=1):
        if axis == 1:
            df_index = self._get_index_axis(axis)
            start = self._get_pos_axis(indexer.start, axis)
            stop = self._get_pos_axis(indexer.stop, axis, False)
            if indexer.step is None or indexer.step > 0:
                return df_index[start:stop + 1:indexer.step]

            if stop > 0:
                return df_index[start:stop - 1:indexer.step]
            if stop == 0 and (start - stop) % abs(indexer.step) == 0:
                cols = df_index[start:stop:indexer.step]
                cols.append(df_index[stop])
                return cols
        else:
            raise NotImplementedError("Not support row slice indexer")


class _LocIndexer(LocationIndexer):
    """ Helper class for the .loc accessor """

    @exe_time
    def _loc(self, iindexer, cindexer):
        """ Helper function for the .loc accessor """
        sdf = self.obj.sdf
        is_single_row = None
        is_single_col = None
        new_df_partition_info = None
        for i, indexer in enumerate([iindexer, cindexer]):
            if com.is_bool_indexer(indexer):
                raise Exception("Not support now")
            if indexer is None or is_empty_slice(indexer):
                if i == 0:
                    is_single_row = self.obj.count() == 1
                else:
                    is_single_col = len(self.obj.columns) == 1
                continue
            if i == 0 and isinstance(indexer, slice):
                keys = self._turn2iloc(indexer)
                is_single_row = (keys.start + 1 == keys.stop)
                sdf, new_df_partition_info = self._get_records_slice(keys)
            else:
                keys = self._label_index2keys(indexer, i)
                if i == 0:
                    is_single_row = False
                else:
                    is_single_col = len(keys) == 1
                sdf = getitem_axis(sdf, keys, i, self.obj.index_col)

        return get_df_or_series(sdf, iindexer, cindexer, self.obj.index_col, isinstance(self.obj, SparkDataFrame),
                                self.obj.name, is_single_row=is_single_row, is_single_col=is_single_col,
                                new_df_partition_info=new_df_partition_info)

    @exe_time
    def _set(self, iindexer, cindexer, value):
        columns = self._label_index2keys(cindexer, axis=1)
        if isinstance(iindexer, slice):
            iindexer = self._turn2iloc(iindexer)
            df = self._set_record_slice(iindexer, columns, value)
        elif isinstance(iindexer, list):
            df = self._set_record_list(iindexer, columns, value)
        elif isinstance(iindexer, int):
            df = self._set_record_list([iindexer], columns, value)
        else:
            raise Exception("Not support row key " + iindexer)

        self.obj._update(df, self.obj.index_col)

    @exe_time
    def _calc_list_partition_value(self, row_keys, columns, values):
        value_infos = []

        if is_single(values):
            return [values] * len(columns)
        elif len(columns) == 1:
            if isinstance(values, list) and len(row_keys) == len(values):
                if is_single(values[0]):
                    value_infos = {}
                    for i in range(len(row_keys)):
                        value_infos[row_keys[i]] = [values[i]]
                elif isinstance(values[0], list):
                    for i in range(len(row_keys)):
                        value_infos[row_keys[i]] = values[i]
                else:
                    raise Exception("rows not equals values")

        elif isinstance(values, list):
            if isinstance(values[0], list):
                if len(row_keys) != len(values):
                    raise Exception("rows not equals values")
                value_infos = {}
                for i in range(len(row_keys)):
                    value_infos[row_keys[i]] = values[i]
            elif is_single(values[0]):
                if len(values) != len(columns):
                    raise Exception("columns size is not equal to values size")
                value_infos = values
            else:
                raise Exception("value_infos in not valid.")
        else:
            raise Exception("invalid columns size and value size")

        return value_infos

    @exe_time
    def _set_record_list(self, row_keys, columns, values):
        value_infos = self._calc_list_partition_value(row_keys, columns, values)
        index_col = self.obj.index_col
        keys_dict = {}
        for i in range(len(row_keys)):
            keys_dict[row_keys[i]] = i

        def f(split_index, iterator):
            def update(origin_data, value):
                if len(columns) != len(value):
                    raise Exception("Update column numbers not equal to value_info")
                new_data = origin_data.asDict()
                for i in range(len(columns)):
                    new_data[columns[i]] = value[i]
                from pyspark.sql.types import Row
                res = Row(*new_data)
                res = res(*new_data.values())
                return res

            record_num = 0
            while iterator:
                record = next(iterator)
                index_name = record.asDict()[index_col]
                offset = keys_dict.get(index_name, -1)
                if offset > -1:
                    if isinstance(value_infos, list):
                        yield update(record, value_infos)
                    elif isinstance(value_infos, dict):
                        yield update(record, value_infos[index_name])
                    record_num += 1
                else:
                    yield record

        return self.obj.sdf.rdd.mapPartitionsWithIndex(f).toDF()

    def _label2pos_by_secondary_index(self, label):
        try:
            from redis import Redis
            #  重构时统一抽取配置Redis
            r = Redis(host='simple27', port=6379, decode_responses=True)
        except Exception as exp:
            raise Exception("write meta_data of dataframe {} to redis failed" % self.obj.df_id)

        return int(r.hmget(self.obj.secondary_index_key_in_redis, label)[0])

    @exe_time
    def _turn2iloc(self, indexer):
        index_col = self.obj.index_col
        if index_col is None:
            return indexer
        if self.obj.already_build_secondary_index:
            start = self._label2pos_by_secondary_index(indexer.start)
            stop = self._label2pos_by_secondary_index(indexer.stop)

            if start == -1 or start is None:
                raise Exception("slice key of " + str(indexer.start) + " is invalid, "
                                + "multiple or none of " + str(indexer.start) + " exists.")
            if stop == -1 or stop is None:
                raise Exception("slice key of " + str(indexer.stop + " is invalid, "
                                + "multiple or none of " + str(indexer.stop) + " exists."))

            if indexer.step is None or indexer.step > 0:
                return slice(start, stop + 1, indexer.step)
            return slice(start, stop - 1, indexer.step)

        pdata, item_num, splitindex_pid_map, pid_splitindex_map = self.obj.meta_data

        def f(split_index, iterator):
            offset = 0
            res = []

            try:
                while iterator:
                    record = next(iterator)
                    data = record.asDict()
                    if indexer.start is not None and str(data[index_col]) == str(indexer.start):
                        res.append((split_index, offset, 0))

                    if indexer.stop is not None and str(data[index_col]) == str(indexer.stop):
                        res.append((split_index, offset, 1))

                    if len(res) == 3:
                        break
                    offset += 1

            except Exception as e:
                pass

            if res != []:
                yield res

        start_repeated = stop_repeated = 0
        if indexer.start is None:
            start_repeated += 1
        if indexer.stop is None:
            stop_repeated += 1

        start_info = None
        stop_info = None
        if start_repeated != 1 or stop_repeated != 1:
            start_stop_info = self.obj.sdf.rdd.mapPartitionsWithIndex(f).collect()
            for p in start_stop_info:
                for info in p:
                    if info[2] == 0:
                        start_info = info
                        start_repeated += 1
                    else:
                        stop_info = info
                        stop_repeated += 1

        if start_repeated != 1:
            raise Exception("slice key of " + str(indexer.start) + " is invalid, "
                            + "multiple or none of " + str(indexer.start) + " exists.")

        if stop_repeated != 1:
            raise Exception("slice key of " + str(indexer.stop) + " is invalid, "
                            + "multiple or none of " + str(indexer.stop) + " exists.")

        if indexer.start is not None:
            start = pdata[splitindex_pid_map[start_info[0]]][0] + start_info[1]
        else:
            start = 0

        if indexer.stop is not None:
            stop = pdata[splitindex_pid_map[stop_info[0]]][0] + stop_info[1]
        else:
            stop = self.obj.shape[0] - 1

        if indexer.step is None or indexer.step > 0:
            return slice(start, stop + 1, indexer.step)

        return slice(start, stop - 1, indexer.step)


class _iLocIndexer(LocationIndexer):
    def __init__(self, obj):
        self.obj = obj

    @property
    def _name(self):
        return self.obj._name

    @exe_time
    def _is_single(self, indexer):
        if isinstance(indexer, slice):
            start = 0 if indexer.start is None else indexer.start
            stop = self.obj.record_nums if indexer.stop is None else indexer.stop
            step = 1 if indexer.step is None else indexer.step
            return start + step == stop
        elif isinstance(indexer, (list, np.ndarray, Series)):
            if isinstance(indexer, (np.ndarray, Series)):
                return len(indexer.tolist()) == 1
            return len(indexer) == 1
        elif is_integer(indexer):
            return True
        elif indexer is None:
            return False
        return False

    @exe_time
    def _loc(self, ilocation, clocation):
        """ Helper function for the .iloc accessor """
        sdf = self.obj.sdf
        new_df_partition_info = None
        for i, location in enumerate([ilocation, clocation]):
            if i == 1:
                try:
                    keys = self._index2keys(location, i)
                except Exception:
                    keys = self._label_index2keys(location, i)
                is_single_col = len(keys) == 1
            else:
                is_single_row = self._is_single(location)

            if location is None or is_empty_slice(location):
                continue

            if i == 0:
                sdf, new_df_partition_info = self._get_records(ilocation)
            else:
                sdf = getitem_axis(sdf, keys, i, self.obj.index_col)

        if sdf is None:
            return self.obj

        return get_df_or_series(sdf, ilocation, clocation, self.obj.index_col, isinstance(self.obj, SparkDataFrame),
                                self.obj.name, is_single_row=is_single_row, is_single_col=is_single_col,
                                new_df_partition_info=new_df_partition_info)

    @exe_time
    def _set(self, iindexer, cindexer, value):
        columns = self._index2keys(cindexer, axis=1)
        if isinstance(iindexer, slice):
            df = self._set_record_slice(iindexer, columns, value)
        elif isinstance(iindexer, list):
            df = self._set_record_list(iindexer, columns, value)
        elif isinstance(iindexer, int):
            df = self._set_record_list([iindexer], columns, value)
        else:
            raise Exception("Not support row key " + iindexer)

        self.obj._update(df, self.obj.index_col)

    @classmethod
    def _calc_list_partition_value(cls, row_ids, columns, values):
        value_infos = []

        if is_single(values):
            return [values] * len(columns)
        elif len(columns) == 1:
            if isinstance(values, list) and len(row_ids) == len(values):
                if is_single(values[0]):
                    value_infos = {}
                    for i in range(len(row_ids)):
                        value_infos[row_ids[i]] = [values[i]]
                elif isinstance(values[0], list):
                    for i in range(len(row_ids)):
                        value_infos[row_ids[i]] = values[i]
                else:
                    raise Exception("rows not equals values")

        elif isinstance(values, list):
            if isinstance(values[0], list):
                if len(row_ids) != len(values):
                    raise Exception("rows not equals values")
                value_infos = {}
                for i in range(len(row_ids)):
                    value_infos[row_ids[i]] = values[i]
            elif is_single(values[0]):
                if len(values) != len(columns):
                    raise Exception("columns size is not equal to values size")
                value_infos = values
            else:
                raise Exception("value_infos in not valid.")
        else:
            raise Exception("invalid columns size and value size")

        return value_infos

    @exe_time
    # 小规模数据，采用map取，取完hash排序
    def _calc_list_partition_id_offset(self, row_ids):
        row_infos = {}
        row_sets = set(row_ids)
        for row_id in row_sets:
            row_info = self._calc_record_partition_id_offset_by_interval_estimation(row_id)
            if row_info[0] in row_infos.keys():
                row_infos[row_info[0]].append((row_id, row_info[1]))
            else:
                row_infos[row_info[0]] = [(row_id, row_info[1])]
        for key in row_infos:
            row_infos[key] = tuple(row_infos[key])
        return row_infos

    @exe_time
    # 小规模数据，采用map
    def _get_records_list(self, row_ids):
        """
           Cannot guarantee results have the same
           order as row_ids's order because the results are collected concurrently.
           And if the data amount is too large, maybe need to be compressed or use rpc.
        """
        row_infos = self._calc_list_partition_id_offset(row_ids)

        if len(row_ids) != len(set(row_ids)):
            new_df_partition_info = None
        else:
            new_df_partition_info = sorted(list(map(lambda x: (x[0], len(x[1])), row_infos.items())), key=lambda x: x[0])

        if self.obj.index_manager is None:
            def f(split_index, iterator):
                if split_index not in row_infos.keys():
                    return

                offsets = {}
                for offset in row_infos[split_index]:
                    offsets[offset[1]] = offset[0]

                offset = 0
                record_num = 0
                while iterator:
                    try:
                        record = next(iterator)
                        if offsets.get(offset, -1) > -1:
                            yield offsets[offset], record
                            record_num += 1

                        offset += 1
                        if record_num == len(row_infos[split_index]):
                            raise StopIteration
                    except StopIteration as e:
                        raise e

            map_list = dict(self.obj.sdf.rdd.mapPartitionsWithIndex(f).collect())
            res_list = list(map(lambda x: map_list[x], row_ids))
            df = spark.createDataFrame(res_list)
            return df, new_df_partition_info
        else:
            def f(x):
                # Todo: 使用Plasma Store
                array_index = x[1].index
                split_index = x[0]

                if split_index not in row_infos.keys():
                    return

                res = []
                # 搜索长度变成了查询的序号个数，按照理论上小于所有数据的长度
                for offset in row_infos[split_index]:
                    res.append((offset[0], array_index[offset[1]]))

                return res

            map_list = dict(self.obj.index_manager.index_rdd.map(f).collect())
            res_list = list(map(lambda x: map_list[x], row_ids))
            df = spark.createDataFrame(res_list)
            return df, new_df_partition_info

    @exe_time
    def _set_record_list(self, row_ids, columns, values):
        row_infos = self._calc_list_partition_id_offset(row_ids)
        value_infos = self._calc_list_partition_value(row_ids, columns, values)

        def update(origin_data, value):
            if len(columns) != len(value):
                raise Exception("Update column numbers not equal to value_info")
            new_data = origin_data.asDict()
            for i in range(len(columns)):
                new_data[columns[i]] = value[i]
            from pyspark.sql.types import Row
            res = Row(*new_data)
            res = res(*new_data.values())
            return res

        if self.obj.index_manager is None:
            def f(split_index, iterator):
                if split_index not in row_infos.keys():
                    while iterator:
                        yield next(iterator)

                offsets = {}
                for offset in row_infos[split_index]:
                    offsets[offset[1]] = offset[0]

                offset = 0
                record_num = 0
                while iterator:
                    record = next(iterator)
                    offset += 1
                    if offsets.get(offset, -1) > -1:
                        if isinstance(value_infos, list):
                            yield update(record, value_infos)
                        elif isinstance(value_infos, dict):
                            yield update(record, value_infos[offsets[offset]])
                        record_num += 1
                    else:
                        yield record

            return self.obj.sdf.rdd.mapPartitionsWithIndex(f).toDF()
        else:
            def f(x):
                array_index = x[1].index
                split_index = x[0]

                if split_index not in row_infos.keys():
                    return array_index

                for offset in row_infos[split_index]:
                    if isinstance(value_infos, list):
                        array_index[offset[1]] = update(array_index[offset[1]], value_infos)
                    elif isinstance(value_infos, dict):
                        array_index[offset[1]] = update(array_index[offset[1]], value_infos[offset[0]])

                return array_index

            return self.obj.index_manager.index_rdd.flatMap(f).toDF()

    @exe_time
    def _get_records(self, ilocation):
        if isinstance(ilocation, slice):
            # Todo：世系关系压缩挪到SymbolDataFrame if self.obj.cur_mapping is not None:
            # return self.get_records_by_mapping_relation_of_origin_df(ilocation)
            return self._get_records_slice(ilocation)
        elif isinstance(ilocation, (list, np.ndarray, Series)):
            return self._get_records_list(ilocation)
        elif is_integer(ilocation):
            return self._get_records_list([ilocation])
        else:
            raise KeyError("Method iloc only support slice, list integers and single integer now!")

    @exe_time
    def _index2keys(self, location, axis=1):
        if isinstance(location, slice):
            return self._slice2keys(location, axis)
        elif isinstance(location, (list, np.ndarray, Series)):
            return self._list2keys(location, axis)
        elif is_integer(location):
            return self._ele2keys(location, axis)
        elif location is None:
            return self._get_index_axis(axis)
        else:
            raise KeyError("Method iloc only support slice, list integers and single integer now!")

    @exe_time
    def _ele2keys(self, location, axis=1):
        self._check_integer_valid(location, axis)
        df_index = self._get_index_axis(axis)
        return [df_index[location]]

    @exe_time
    def _list2keys(self, location, axis=1):
        df_index = self._get_index_axis(axis)
        if isinstance(location, (np.ndarray, Series)):
            location = location.tolist()
        if com.is_bool_indexer(location):
            return self._bool2keys(location, axis)
        keys = []
        for i in location:
            self._check_integer_valid(i, axis)
            keys.append(df_index[i])
        return keys

    @exe_time
    def _slice2keys(self, location, axis=1):
        df_index = self._get_index_axis(axis)
        start = location.start if location.start is not None else 0
        if not is_integer(start):
            raise KeyError("Key of %s does not exists." % start)

        stop = location.stop if location.stop is not None else len(df_index)
        if not is_integer(stop):
            raise KeyError("Key of %s does not exists." % stop)

        step = 1 if location.step is None else location.step
        if not is_integer(step):
            raise KeyError("Slice step of %s is not valid." % step)

        keys = df_index[start:stop:step]
        return keys


