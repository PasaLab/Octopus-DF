#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append("/Users/jun/workspace/test/Octopus-DataFrame/Octopus")
from Octopus.dataframe.core.methods import read_csv, from_pandas
from Octopus.dataframe.core.sparkinit import spark
from pyspark.sql import functions
from pyspark.sql.functions import udf
from Octopus.dataframe.core.sparkSeries import Series
from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
# path1 = "/Users/jun/Desktop/testcsv/data1.csv"
from pyspark.sql.types import IntegerType
import logging
# logging.getLogger().setLevel(logging.INFO)
# logging.basicConfig(level=logging.INFO,
#                     format='%(levelname)s: %(filename)s:line %(lineno)s:%(message)s')
from Octopus.dataframe.core.symbolDataFrame import SymbolDataFrame
path = '/Users/jun/workspace/OctDataFrame/tests/dataframe/test_data/MDOrderRecord.csv'
df = SymbolDataFrame.from_csv(path, engine_type="Spark")
df1 = df.set_index('MDDate').loc[[20170816], :]
print(df1.compute())
# df['MDDate'] = '1.0'
# print(df.compute())


# df1 = df.set_index("MDDate")
# df1.loc[20170822:20170823:2, :] = 1
# print(df1.compute())
# df1.loc[20170822:20170823:3, :] = 2
# print(df1.compute())

# df.iloc[3:5, :] = 1
# print(df.compute())
# df.iloc[4:6, :] = 2
# print(df.compute())

# df1 = df['MDDate']
# print(df1.compute())

# list
# df1 = df.set_index('MDDate').loc[[20170816], :]
# print(df1.compute())


# slice
# df1 = df.set_index('MDDate').loc["20170823":"20170822":-2, :]
# print(df1.compute())


# list
# df1 = df.iloc[[1,2,1], [1, 3, 4]]
# print(df1.compute())


# slice
# df1 = df.iloc[3:0:-1, 3:1:-1]
# print(df1.compute())


# df1 = df.set_index('MDDate').filter(like="23", axis=0)
# print(df1.compute())

# df1 = df.drop(columns=["MDDate", "MDTime"])
# print(df1.compute())


# df1 = df.tail(5)
# print(df1.compute())

# df1 = df.head(5)
# print(df1.compute())

# sdf = SymbolDataFrame.from_csv(path, engine_type="Spark")
# print(df.compute())
# df1 = df.set_index("MDDate")
# print(df1.compute())
# df2 = df.join(df, how="left", lsuffix='l', rsuffix='r')
# sdf2 = sdf.join(sdf, how="left", lsuffix='l', rsuffix='r')
# print(df2.compute())
# print(sdf2.compute())

# df1 = df.sort_values(by=["MDDate"])
# print(df1.compute())

# df1 = df.set_index(keys="MDTime")
# print(df1.compute())


# df1 = df.drop_duplicates()
# df2 = df.drop_duplicates(["MDTime"])
# print(df1.compute())
# print(df2.compute())


# df1 = df.min()
# df2 = df.max()
# df3 = df.mean()
# df4 = df.count()
# df5 = df.sum()
# print(df1.compute())
# print(df2.compute())
# print(df3.compute())
# print(df4.compute())
# print(df5.compute())


# df1 = df.groupby(by="ID(HTSCSecurityID)").min()
# df2 = df.groupby(by="ID(HTSCSecurityID)").max()
# df3 = df.groupby(by="ID(HTSCSecurityID)").mean()
# df4 = df.groupby(by="ID(HTSCSecurityID)").count()
# df5 = df.groupby(by="ID(HTSCSecurityID)").sum()
# print(df1.compute())
# print(df2.compute())
# print(df3.compute())
# print(df4.compute())
# print(df5.compute())

# import time
# t0 = time.time()
# df = SymbolDataFrame.from_csv(path2)
# df1 = df.iloc[0:8:1, :]
# df2 = df.iloc[1:5:1, :]
# df3 = df1.join(df2, on="MDDate", rsuffix='r', lsuffix='l')
# print(df3.compute())
# print(time.time() - t0)


#
# odf = read_csv(path2, is_cache=True)
# print(odf)
# odf2 = odf.set_index('ID(HTSCSecurityID)', use_local_index=True)
# print(odf2)
# print(odf2.iloc[0:6:1, :])  # 结果不对， bug
# odf2.index_manager.drop_index()
# odf = odf.set_index('日期')
# import time
# t0 = time.time()


# 其实是小块查询的性能 一定比先查询大块，再在大块上查询小块要块
# print(odf.iloc[5:56:10])

# import octopus
# path2 = '/Users/jun/workspace/OctDataFrame/tests/dataframe/test_data/salary.csv'
# path3 = '/Users/jun/workspace/OctDataFrame/tests/dataframe/test_data/attendance.csv'
#
# salary = octopus.read_csv(path2)
# attendance = octopus.read_csv(path3)
# bonus_employee= salary.loc["000001":"001000":1]["salary"]
# attendance_count = attendance.loc["20180101":"2018131":1]\
#     .groupby("employeeId")\
#     .agg({'checkIn': 'count'})
# bonus_data = bonus_employee.merge(attendance_count, on="employeeId")
# bonus_data["bonus"] = bonus_data["salary"] * bonus_data["checkIn"]
# print(bonus_data)
