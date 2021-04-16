#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/3 21:03
# @Author  : Willpower-chen
# @Site    : 
# @File    : format.py
# @Software: PyCharm


import pandas as pd
import os

input_base_path = "/Users/jun/workspace/OctDataFrame/Octopus/dataframe/predict/training_data/Spark"

example_path = "/Users/jun/workspace/OctDataFrame/Octopus/dataframe/predict/training_data/max_copy.csv"

df = pd.read_csv(example_path)

rows = list(df["row"])
cols = list(df["col"])
print(rows, cols)

for filename in os.listdir(input_base_path):
    df = pd.read_csv(input_base_path + "/" + filename, header=-1)
    size = df.size
    print(df)
    print("size", size)
    df["rows"] = rows[0: size]
    df["cols"] = cols[0:size]
    df["time"] = df[0]
    update_df = df.loc[:, ["rows", "cols", "time"]]
    print(update_df)
    update_df.to_csv(path_or_buf=input_base_path + "1/" + filename, mode="w", index=False)



