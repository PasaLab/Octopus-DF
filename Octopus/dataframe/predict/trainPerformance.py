#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from scipy.optimize import leastsq

base_input_path = "/Users/jun/workspace/OctDataFrame/Octopus/dataframe/predict/training_data"
base_output_path = "/Users/jun/workspace/OctDataFrame/Octopus/dataframe/predict/models"


def read_traning_data(path):
    df = pd.read_csv(path, sep=',', header=-1)
    rows = np.array(df.iloc[:, 0])
    cols = np.array(df.iloc[:, 1])
    time = np.array(df.iloc[:, 2])
    return rows, cols, time


def error(p, row, col, time, func, platform):
    return func(p, row, col, platform) - time  # x、y都是列表，故返回值也是个列


def train_join(platform):
    input_path = base_input_path + "/" + platform + "/join.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row1, row2, platform):
        print("join iteration")
        k, b = p
        return k * (row1*row2) + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform)) #把error函数中除了p以外的参数打包到args中
    k, b = result[0]
    output_path = base_output_path + "/" + platform + "/join.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k, b, file=file, sep=",")
    # print("k1=",k1, '\n',"k2=", k2, '\n', "k3=", k3, '\n', "b=",b)


def train_sort(platform):
    input_path = base_input_path + "/" + platform + "/sort_values.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("sort iteration ", platform)
        k1, k2, b = p
        return k1 * row  + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/sort_values.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_iloc(platform):
    input_path = base_input_path + "/" + platform + "_num/iloc.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k, b = p
        return k * row + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k, b = result[0]
    output_path = base_output_path + "/" + platform + "/iloc.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k, b, file=file, sep=",")
    pass


def train_loc(platform):
    input_path = base_input_path + "/" + platform + "/loc.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k, b = p
        return k * row + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k, b = result[0]
    output_path = base_output_path + "/" + platform + "/loc.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k, b, file=file, sep=",")


def train_filter(platform):
    input_path = base_input_path + "/" + platform + "/filter.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k1, k2, b = p
        return k1 * row + k2 * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/filter.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_drop_duplicates(platform):
    input_path = base_input_path + "/" + platform + "/drop_duplicates.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k1, k2, b = p
        return k1 * row + k2 * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/drop_duplicates.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_min(platform):
    input_path = base_input_path + "/" + platform + "/min.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k1, k2, b = p
        return k1 * row + k2*col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/min.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_sum(platform):
    input_path = base_input_path + "/" + platform + "/sum.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("sum iteration ", platform)
        k1, k2, b = p
        return k1 * row*col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/sum.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_mean(platform):
    input_path = base_input_path + "/" + platform + "/mean.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k1, k2, b = p
        return k1 * row + k2 * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/mean.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_max(platform):
    input_path = base_input_path + "/" + platform + "/max.csv"
    rows, cols, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("join iteration")
        k1, k2, b = p
        return k1 * row + k2 * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows, cols, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/max.csv"
    file = open(output_path, 'w')
    print(k1, k2, b, file=file, sep=",")


def train_groubpby_min(platform):
    input_path = base_input_path + "/" + platform + "/groupbymin.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k1, k2, b = p
        return k1 * row + k2 * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/groupbymin.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_groubpby_sum(platform):
    input_path = base_input_path + "/" + platform + "/groupbysum.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k1, k2, b = p
        return k1 * row + k2 * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/groupbysum.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_groubpby_max(platform):
    input_path = base_input_path + "/" + platform + "/groupbymax.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k1, k2, b = p
        return k1 * row + k2 * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/groupbymax.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_groubpby_mean(platform):
    input_path = base_input_path + "/" + platform + "/groupbymean.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("loc iteration ", platform)
        k1, k2, b = p
        return k1 * row + k2*col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10, 1]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k1, k2, b = result[0]
    output_path = base_output_path + "/" + platform + "/groupbymean.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k1, k2, b, file=file, sep=",")


def train_read_hdfs_parquet(platform):
    platform = platform.lower()
    input_path = base_input_path + "/transfer/" + platform + "_" + "read_hdfs.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("transfer read iteration ", platform)
        k, b = p
        return k * row * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k, b = result[0]
    output_path = base_output_path + "/transfer/" + platform + "_read_hdfs.csv"
    file = open(output_path, 'w')
    print(k, b, file=file, sep=",")


def train_write_hdfs_parquet(platform):
    platform = platform.lower()
    input_path = base_input_path + "/transfer/" + platform + "_" + "write_hdfs.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("transfer write iteration ", platform)
        k, b = p
        return k * row * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k, b = result[0]
    output_path = base_output_path + "/transfer/" + platform + "_write_hdfs.csv"
    file = open(output_path, 'w')
    # print("k","b",file=file, sep=',')
    print(k, b, file=file, sep=",")


def train_read_hdfs_csv(platform):
    platform = platform.lower()
    input_path = base_input_path + "/" + platform + "_num/read_csv.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("read csv iteration ", platform)
        k, b = p
        return k * row * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k, b = result[0]
    output_path = base_output_path + "/" + platform + "/read_csv.csv"
    file = open(output_path, 'w')
    print(k, b, file=file, sep=",")


def train_write_hdfs_csv(platform):
    platform = platform.lower()
    input_path = base_input_path + "/" + platform + "/write_csv.csv"
    rows1, rows2, time = read_traning_data(input_path)

    def func(p, row, col, platform):
        print("transfer write iteration ", platform)
        k, b = p
        return k * row * col + b

    # 试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
    p0 = [10, 10]
    result = leastsq(error, p0, args=(rows1, rows2, time, func, platform))  # 把error函数中除了p以外的参数打包到args中
    k, b = result[0]
    output_path = base_output_path + "/" + platform + "/write_csv.csv"
    file = open(output_path, 'w')
    print(k, b, file=file, sep=",")


for p in ["Dask", "Pandas", "Spark"]:
    print(p)
    # train_join(p)
    # train_max(p)
    # train_sort(p)
    # train_loc(p)
    # train_drop_duplicates(p)
    # train_max(p)
    # train_min(p)
    # train_sum(p)
    # train_mean(p)
    # train_groubpby_max(p)
    # train_groubpby_mean(p)
    # train_groubpby_sum(p)
    # train_groubpby_mean(p)
    # train_read_hdfs_parquet(p)
    # train_write_hdfs_paruqet(p)
    # train_sort(p)
    # train_iloc(p)
    train_read_hdfs_csv(p)


# def error(p, row, col, time, func_type, platform):
#     print(platform, ".", func_type)
#     return func(p, row, col, func_type, platform) - time #x、y都是列表，故返回值也是个列表

# #TEST
# p0=[1, 1, 1, 1]
# #print( error(p0,Xi,Yi) )
#
# ###最小二乘法试验###
# s="Test the number of iteration" #试验最小二乘法函数leastsq得调用几次error函数才能找到使得均方误差之和最小的k、b
# Para = leastsq(error, p0, args=(rows, cols, time, s)) #把error函数中除了p以外的参数打包到args中
# k1, k2, k3, b = Para[0]
# print("k1=",k1,'\n',"k2=",k2,'\n', "k3=",k3,'\n', "b=",b)
#
#
# print(func(Para[0], 100000, 20))
# print(func(Para[0], 250000,10))
# print(func(Para[0], 300000,10))
