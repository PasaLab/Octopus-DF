#!/usr/bin/env python
# -*- coding: utf-8 -*-

base_path = "/Users/jun/workspace/OctDataFrame/Octopus/dataframe/predict/models"
# base_path = "/home/shijun/jun/anaconda3/envs/testdask/lib/python3.6/site-packages/Octopus/dataframe/predict/models"


def get_operator_time_by_size(platform, operator, *args):
    if operator == 'from_csv':
        operator = "read_csv"
    path = base_path + "/" + platform + "/" + operator + ".csv"
    with open(path, 'r') as f:
        paras = list(map(lambda x: float(x), f.readline().strip("\n").split(',')))

    if operator == "sum":
        return paras[0]*args[0]*args[1] + paras[2]

    if len(args) < 1:
        raise Exception("performance input params should not less than 1")
    if len(paras) == 2:
        return (float)(paras[0])*(float)(args[0]) + (float)(paras[1])
    elif len(paras) == 3:
        if len(args) == 1:
            exp = platform + "." + operator + "should have 2 params"
            raise Exception(exp)

        return paras[0]*args[0] + paras[1]*args[1] + paras[2]
    else:
        raise Exception("paras should be less 3")


def get_trans_time_by_size(source, target, *args):
    source = source.lower()
    target = target.lower()
    # 先写后读
    path_write_model = base_path + "/transfer/" + source + "_write_hdfs.csv"
    path_read_model = base_path + "/transfer/" + target + "_read_hdfs.csv"

    with open(path_write_model, 'r') as f:
        paras_write = list(map(lambda x: float(x), f.readline().strip("\n").split(',')))

    if len(args) != 2 or len(paras_write) != 2:
        raise Exception("args length should be 2")

    write_time = paras_write[0]*(args[0] * args[1]) + paras_write[1]

    with open(path_read_model, 'r') as f:
        paras_read = list(map(lambda x: float(x), f.readline().strip("\n").split(',')))

    if len(args) != 2 or len(paras_read) != 2:
        raise Exception("args length should be 2")

    read_time = paras_read[0]*(args[0] * args[1]) + paras_read[1]

    return write_time + read_time

# a = get_operator_time_by_size("Pandas", "max", 10000, 1000)
# print(a)
# b = get_operator_time_by_size("Spark", "join", 100, 100)
# print(b)

