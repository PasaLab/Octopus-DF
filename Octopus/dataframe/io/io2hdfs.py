#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import uuid
import dask
import logging
import pyarrow as pa
import pyarrow.parquet as pq

fs = pa.hdfs.connect(user="experiment")
hdfs_header = "hdfs://simple27:8020/"


def pandas_write_hdfs(pdf):
    t0 = time.time()
    adf = pa.Table.from_pandas(pdf.dataframe)
    random_id = uuid.uuid4()
    name = str(random_id) + ".parquet"
    path = hdfs_header + "shijun/tmp/parquets/" + name
    with fs.open(path, "wb") as fw:
        pq.write_table(adf, fw)
    t2 = time.time()
    logging.debug("write pandas to hdfs cost time of {}" % t2 - t0)
    return path


def pandas_read_hdfs(path):
    t0 = time.time()
    pdf = None
    with fs.open(path, "rb") as fw:
        pdf = pq.read_pandas(fw).to_pandas()
    t2 = time.time()
    logging.debug("read pandas from hdfs cost time of {}" % (t2 - t0))
    from Octopus.dataframe.core.pandasDataFrame import PandasDataFrame
    return PandasDataFrame(pdf)


def dask_write_hdfs(ddf):
    from dask.distributed import Client
    import dask
    # client = Client(n_workers=4, threads_per_worker=24, processes=True, memory_limit="25GB")
    # import dask.dataframe as dd
    t0 = time.time()
    random_id = uuid.uuid4()
    name = str(random_id) + ".parquet"
    path = hdfs_header + "shijun/tmp/parquets/" + name
    ddf = ddf.dataframe.to_parquet(path)
    t2 = time.time()
    logging.debug("write dask to hdfs cost time of {}" % (t2 - t0))
    return path


def dask_read_hdfs(path):
    from dask.distributed import Client
    client = Client(n_workers=4, threads_per_worker=24, processes=True, memory_limit="25GB")
    import dask.dataframe as dd
    t0 = time.time()
    ddf = dd.read_parquet(path, engine="pyarrow")
    t2 = time.time()
    print("read dask from hdfs cost time of ", t2 - t0)
    from Octopus.dataframe.core.daskDataFrame import DaskDataFrame
    return DaskDataFrame(ddf)


def spark_read_hdfs(path):
    t0 = time.time()
    from Octopus.dataframe.core.methods import read_parquet
    sdf = read_parquet(path)
    t2 = time.time()
    print("read spark from hdfs cost time of ", t2 - t0)
    return sdf


def spark_write_hdfs(sdf):
    t0 = time.time()
    random_id = uuid.uuid4()
    name = str(random_id) + ".parquet"
    path = hdfs_header + "shijun/tmp/parquets/" + name
    sdf.to_parquet(path, engine='pyarrow')
    t2 = time.time()
    print("write spark to hdfs cost time of ", t2 - t0)
    return path



