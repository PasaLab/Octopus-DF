#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 06/12/2017 11:26
# @Author  : Willpower-chen
# @Site    : 
# @File    : test_series.py
# @Software: PyCharm

from Octopus.dataframe.core import methods, sparkDataFrame
import pyspark
import math
path = '../../../tests/dataframe/test_data/MDOrderRecord.csv'
path_col = '../../../tests/dataframe/test_data/ColSeries.csv'
path_row = '../../../tests/dataframe/test_data/RowSeries.csv'
path2 = '../../../tests/dataframe/test_data/MDStockRecord.csv'
path_num1 = '../../../tests/dataframe/test_data/1.csv'
path_num2 = '../../../tests/dataframe/test_data/2.csv'

#####################series 操作
odf = methods.read_csv(path).loc[:, 'OrderQty']
odf1 = methods.read_csv(path).loc[:, 'MDDate']
print(odf)
print(odf1)
print(odf + 2)
print(odf - 2)
print(odf / 2)
print(odf // 2)
print(odf * 2)
print(odf % 2)
print(odf ** 2)  ##pow运算
print(odf + odf1)
print(odf - odf1)
print(odf * odf1)
print(odf / odf1)
print(odf // odf1)
print(odf % odf1)

print(odf > 1600)
print(odf >= 1600)
print(odf == 1600)
print(odf < 1600)
print(odf <= 1600)
print(odf != 1600)

print(odf > odf1)
print(odf >= odf1)
print(odf == odf1)
print(odf < odf1)
print(odf <= odf1)
print(odf != odf1)

print(odf & False)
print(odf | True)

print((odf < 2000) & (odf > 1300))
print((odf <= 1600) | (odf >= 2000))