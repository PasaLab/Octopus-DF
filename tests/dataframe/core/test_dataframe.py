import os
import pandas as pd
from configparser import ConfigParser

from Octopus.dataframe.core import Series
from Octopus.dataframe.core import methods
from Octopus.dataframe.core import SparkDataFrame
from Octopus.dataframe.core.utils import compare_df

conf = ConfigParser()
user_home = os.path.expanduser('~')
conf_path = user_home + "/.config/octopus/config.ini"
conf.read(conf_path)

DATA_DIR = conf.get('test', 'DATA_DIR')

cur_dir = os.path.dirname(os.path.realpath(__file__))
path_pd = cur_dir + "/../test_data/MDOrderRecord.csv"
path_pd2 = cur_dir + "/../test_data/MDStockRecord.csv"


pdf = pd.read_csv(path_pd)
pdf = pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True)
pdf2 = pd.read_csv(path_pd2)
pdf2 = pdf2.sort_values(by=list(pdf2.columns)).reset_index(drop=True)

odf = methods.from_pandas(pdf)

print("origin odf", odf)
odf.to_csv(DATA_DIR + "/MDOrderRecord.csv")
odf = methods.read_csv(DATA_DIR + "/MDOrderRecord.csv").sort_values(by=list(odf.columns)).reset_index(drop=True)

# odf = methods.read_csv(path_pd)
# odf = odf.sort_values(by=list(odf.columns)).reset_index(drop=True)

odf2 = methods.from_pandas(pdf2)
odf2.to_csv(DATA_DIR + "/MDStockRecord.csv")
odf2 = methods.read_csv(DATA_DIR + "/MDStockRecord.csv").sort_values(by=list(odf2.columns)).reset_index(drop=True)
# odf2 = methods.read_csv(path_pd)
# odf2 = odf2.sort_values(by=list(odf2.columns)).reset_index(drop=True)

# pdf = pdf.reset_index(drop=True)
# pdf2 = pdf2.reset_index(drop=True)

print("odf", odf)
print("pdf", pdf)
# print(odf.meta_data)
# print(odf.loc[0:0])


class TestDataframe:
    def test_loc(self):
        row1 = odf.loc[0]
        row2 = pdf.loc[0]
        row11 = odf.loc[0, :]
        row22 = pdf.loc[0, :]
        row3 = odf.loc[0:0]
        row4 = pdf.loc[0:0]
        row33 = odf.loc[0:0, :]
        row44 = pdf.loc[0:0, :]
        row5 = odf.loc[[0]]
        row6 = pdf.loc[[0]]
        row55 = odf.loc[[0], :]
        row66 = pdf.loc[[0], :]

        col1 = odf.loc[:, 'MDDate']
        col2 = pdf.loc[:, 'MDDate']
        col3 = odf.loc[:, 'MDDate':'MDDate']
        col4 = pdf.loc[:, 'MDDate':'MDDate']
        col5 = odf.loc[:, ['MDDate']]
        col6 = pdf.loc[:, ['MDDate']]

        row_col1 = odf.loc[2, 'MDDate']
        row_col2 = pdf.loc[2, 'MDDate']
        # print('*'* 40)
        # print(odf)
        # print(pdf)
        # print(row_col1)
        # print(row_col2)
        # print('*'*40)

        row_col3 = odf.loc[2, 'MDDate':'MDTime']
        row_col4 = pdf.loc[2, 'MDDate':'MDTime']
        row_col5 = odf.loc[2, ['MDDate', 'MDTime']]
        row_col6 = pdf.loc[2, ['MDDate', 'MDTime']]

        row_col7 = odf.loc[[0, 2, 4], 'MDDate']
        row_col8 = pdf.loc[[0, 2, 4], 'MDDate']
        row_col9 = odf.loc[[0, 2, 4], 'MDDate':'MDTime']
        row_col10 = pdf.loc[[0, 2, 4], 'MDDate':'MDTime']
        row_col11 = odf.loc[[0, 2, 4], ['MDDate', 'MDTime']]
        row_col12 = pdf.loc[[0, 2, 4], ['MDDate', 'MDTime']]

        row_col13 = odf.loc[2:4, 'MDDate']
        row_col14 = pdf.loc[2:4, 'MDDate']
        row_col15 = odf.loc[2:4, 'MDDate':'MDTime']
        row_col16 = pdf.loc[2:4, 'MDDate':'MDTime']
        row_col17 = odf.loc[2:4, ['MDDate', 'MDTime']]
        row_col18 = pdf.loc[2:4, ['MDDate', 'MDTime']]

        assert row1.tolist() == row2.tolist()
        assert isinstance(row1, Series)
        assert row11.tolist() == row22.tolist()
        assert isinstance(row11, Series)
        assert compare_df(row3.to_pandas().drop(columns='index', errors='ignore'), row4)
        assert isinstance(row3, SparkDataFrame)
        assert compare_df(row33.to_pandas().drop(columns='index', errors='ignore'), row44)
        assert isinstance(row33, SparkDataFrame)
        assert compare_df(row5.to_pandas().drop(columns='index', errors='ignore'), row6)
        assert isinstance(row5, SparkDataFrame)
        assert compare_df(row55.to_pandas().drop(columns='index', errors='ignore'), row66)
        assert isinstance(row55, SparkDataFrame)

        assert col1.tolist() == col2.tolist()
        assert isinstance(col1, Series)
        assert compare_df(col3.to_pandas().drop(columns='index', errors='ignore'), col4)
        assert isinstance(col3, SparkDataFrame)
        assert compare_df(col5.to_pandas().drop(columns='index', errors='ignore'), col6)
        assert isinstance(col5, SparkDataFrame)

        assert row_col1 == row_col2
        assert isinstance(row_col1, np.int64)
        assert row_col3.tolist() == row_col4.tolist()
        assert isinstance(row_col3, Series)
        assert row_col5.tolist() == row_col6.tolist()
        assert isinstance(row_col5, Series)
        assert row_col7.tolist() == row_col8.tolist()
        assert isinstance(row_col7, Series)
        assert compare_df(row_col9.to_pandas().drop(columns='index', errors='ignore'), row_col10)
        assert isinstance(row_col9, SparkDataFrame)
        assert compare_df(row_col11.to_pandas().drop(columns='index', errors='ignore'), row_col12)
        assert isinstance(row_col11, SparkDataFrame)
        assert row_col13.tolist() == row_col14.tolist()
        assert isinstance(row_col13, Series)
        assert compare_df(row_col15.to_pandas().drop(columns='index', errors='ignore'), row_col16)
        assert isinstance(row_col15, SparkDataFrame)
        assert compare_df(row_col17.to_pandas().drop(columns='index', errors='ignore'), row_col18)
        assert isinstance(row_col17, SparkDataFrame)

    def test_iloc(self):
        row1 = odf.iloc[2]
        row2 = pdf.iloc[2]
        row11 = odf.iloc[2, :]
        row22 = pdf.iloc[2, :]
        row3 = odf.iloc[2:3]
        row4 = pdf.iloc[2:3]
        row33 = odf.iloc[2:3, :]
        row44 = pdf.iloc[2:3, :]
        row5 = odf.iloc[[2]]
        row6 = pdf.iloc[[2]]
        row55 = odf.iloc[[2], :]
        row66 = pdf.iloc[[2], :]

        col1 = odf.iloc[:, 1]
        col2 = pdf.iloc[:, 1]
        col3 = odf.iloc[:, 1:2]
        col4 = pdf.iloc[:, 1:2]
        col5 = odf.iloc[:, [1]]
        col6 = pdf.iloc[:, [1]]

        row_col1 = odf.iloc[2, 1]
        row_col2 = pdf.iloc[2, 1]
        row_col3 = odf.iloc[2, 1:4]
        row_col4 = pdf.iloc[2, 1:4]
        row_col5 = odf.iloc[2, [1, 3]]
        row_col6 = pdf.iloc[2, [1, 3]]

        row_col7 = odf.iloc[[0, 2, 4], 1]
        row_col8 = pdf.iloc[[0, 2, 4], 1]
        row_col9 = odf.iloc[[0, 2, 4], 1:4]
        row_col10 = pdf.iloc[[0, 2, 4], 1:4]
        row_col11 = odf.iloc[[0, 2, 4], [1, 3]]
        row_col12 = pdf.iloc[[0, 2, 4], [1, 3]]

        row_col13 = odf.iloc[2:4, 1]
        row_col14 = pdf.iloc[2:4, 1]
        row_col15 = odf.iloc[2:4, 1:4]
        row_col16 = pdf.iloc[2:4, 1:4]
        row_col17 = odf.iloc[2:4, [1, 3]]
        row_col18 = pdf.iloc[2:4, [1, 3]]

        assert row1.tolist() == row2.tolist()
        assert isinstance(row1, Series)
        assert row11.tolist() == row22.tolist()
        assert isinstance(row11, Series)
        assert compare_df(row3.to_pandas().drop(columns='index', errors='ignore'), row4)
        assert isinstance(row3, SparkDataFrame)
        assert compare_df(row33.to_pandas().drop(columns='index', errors='ignore'), row44)
        assert isinstance(row33, SparkDataFrame)
        assert compare_df(row5.to_pandas().drop(columns='index', errors='ignore'), row6)
        assert isinstance(row5, SparkDataFrame)
        assert compare_df(row55.to_pandas().drop(columns='index', errors='ignore'), row66)
        assert isinstance(row55, SparkDataFrame)

        assert col1.tolist() == col2.tolist()
        assert isinstance(col1, Series)
        assert compare_df(col3.to_pandas().drop(columns='index', errors='ignore'), col4)
        assert isinstance(col3, SparkDataFrame)
        assert compare_df(col5.to_pandas().drop(columns='index', errors='ignore'), col6)
        assert isinstance(col5, SparkDataFrame)

        assert row_col1 == row_col2
        assert isinstance(row_col1, np.int64)
        assert row_col3.tolist() == row_col4.tolist()
        assert isinstance(row_col3, Series)
        assert row_col5.tolist() == row_col6.tolist()
        assert isinstance(row_col5, Series)
        assert row_col7.tolist() == row_col8.tolist()
        assert isinstance(row_col7, Series)
        assert compare_df(row_col9.to_pandas().drop(columns='index', errors='ignore'), row_col10)
        assert isinstance(row_col9, SparkDataFrame)
        assert compare_df(row_col11.to_pandas().drop(columns='index', errors='ignore'), row_col12)
        assert isinstance(row_col11, SparkDataFrame)
        assert row_col13.tolist() == row_col14.tolist()
        assert isinstance(row_col13, Series)
        assert compare_df(row_col15.to_pandas().drop(columns='index', errors='ignore'), row_col16)
        assert isinstance(row_col15, SparkDataFrame)
        assert compare_df(row_col17.to_pandas().drop(columns='index', errors='ignore'), row_col18)
        assert isinstance(row_col17, SparkDataFrame)

    def test_bracket(self):
        col1 = odf['MDDate']
        col2 = pdf['MDDate']

        col3 = odf[['MDDate', 'MDTime']]
        col4 = pdf[['MDDate', 'MDTime']]

        assert col1.tolist() == col2.tolist()
        assert compare_df(col3, col4)

    def test_head(self):
        h1 = odf.head(5)
        h2 = pdf.head(5)
        assert compare_df(h1.to_pandas().drop(columns='index', errors='ignore'), h2, index='ID(HTSCSecurityID)')

    def test_tail(self):
        t1 = odf.tail(5)
        t2 = pdf.tail(5)
        assert compare_df(t1.to_pandas().drop(columns='index', errors='ignore'), t2, index='ID(HTSCSecurityID)')

    def test_shape(self):
        s1 = odf.shape
        # print("s1", s1)
        s2 = pdf.shape
        # print("s2", s2)
        assert s1 == s2

    def test_describe(self):
        des = odf.describe()
        if odf.use_pdf:
            assert des.shape == (8, 3)
        else:
            assert des.shape == (5, len(odf.columns))

    def test_max(self):
        m = odf.max()
        assert len(m.tolist()) == len(odf.columns)

    def test_min(self):
        m = odf.min()
        assert len(m.tolist()) == len(odf.columns)

    def test_mean(self):
        m = odf.mean()
        if odf.use_pdf:
            assert len(m.tolist()) == 3
        else:
            assert len(m.tolist()) == len(odf.columns)

    def test_sum(self):
        m = odf.sum()
        # print(m)
        # if odf.use_pdf:
        #     assert len(m.tolist()) == 3
        # else:
        assert len(m.tolist()) == len(odf.columns)

    def test_merge(self):
        d1 = odf.merge(odf2, how='inner', on=['ID(HTSCSecurityID)', 'MDDate'])
        d1 = d1.sort_values(by=list(d1.columns))
        d2 = pdf.merge(pdf2, how='inner', on=['ID(HTSCSecurityID)', 'MDDate'])
        d2 = d2.sort_values(by=list(d2.columns))
        d11 = odf.merge(odf2, how='outer', on=['ID(HTSCSecurityID)', 'MDDate'])
        d11 = d11.sort_values(by=list(d11.columns))
        d21 = pdf.merge(pdf2, how='outer', on=['ID(HTSCSecurityID)', 'MDDate'])
        d21 = d21.sort_values(by=list(d21.columns))
        d12 = odf.merge(odf2, how='left', on=['ID(HTSCSecurityID)', 'MDDate'])
        d12 = d12.sort_values(by=list(d12.columns))
        d22 = pdf.merge(pdf2, how='left', on=['ID(HTSCSecurityID)', 'MDDate'])
        d22 = d22.sort_values(by=list(d22.columns))
        d13 = odf.merge(odf2, how='right', on=['ID(HTSCSecurityID)', 'MDDate'])
        d13 = d13.sort_values(by=list(d13.columns))
        d23 = pdf.merge(pdf2, how='right', on=['ID(HTSCSecurityID)', 'MDDate'])
        d23 = d23.sort_values(by=list(d23.columns))

        d31 = odf.merge(odf2, how='inner', left_on='ID(HTSCSecurityID)', right_on='ID(HTSCSecurityID)')
        d31 = d31.sort_values(by=list(d31.columns))
        d41 = pdf.merge(pdf2, how='inner', left_on='ID(HTSCSecurityID)', right_on='ID(HTSCSecurityID)')
        d41 = d41.sort_values(by=list(d41.columns))
        d32 = odf.merge(odf2, how='outer', left_on='ID(HTSCSecurityID)', right_on='ID(HTSCSecurityID)')
        d32 = d32.sort_values(by=list(d32.columns))
        d42 = pdf.merge(pdf2, how='outer', left_on='ID(HTSCSecurityID)', right_on='ID(HTSCSecurityID)')
        d42 = d42.sort_values(by=list(d42.columns))
        d33 = odf.merge(odf2, how='left', left_on='ID(HTSCSecurityID)', right_on='ID(HTSCSecurityID)')
        d33 = d33.sort_values(by=list(d33.columns))
        d43 = pdf.merge(pdf2, how='left', left_on='ID(HTSCSecurityID)', right_on='ID(HTSCSecurityID)')
        d43 = d43.sort_values(by=list(d43.columns))
        d34 = odf.merge(odf2, how='right', left_on='ID(HTSCSecurityID)', right_on='ID(HTSCSecurityID)')
        d34 = d34.sort_values(by=list(d34.columns))
        d44 = pdf.merge(pdf2, how='right', left_on='ID(HTSCSecurityID)', right_on='ID(HTSCSecurityID)')
        d44 = d44.sort_values(by=list(d44.columns))

        d51 = odf.merge(odf2, how='inner')
        d51 = d51.sort_values(by=list(d51.columns))
        d61 = pdf.merge(pdf2, how='inner')
        d61 = d61.sort_values(by=list(d61.columns))
        d52 = odf.merge(odf2, how='outer')
        d52 = d52.sort_values(by=list(d52.columns))
        d62 = pdf.merge(pdf2, how='outer')
        d62 = d62.sort_values(by=list(d62.columns))
        d53 = odf.merge(odf2, how='left')
        d53 = d53.sort_values(by=list(d53.columns))
        d63 = pdf.merge(pdf2, how='left')
        d63 = d63.sort_values(by=list(d63.columns))
        d54 = odf.merge(odf2, how='right')
        d54 = d54.sort_values(by=list(d54.columns))
        d64 = pdf.merge(pdf2, how='right')
        d64 = d64.sort_values(by=list(d64.columns))

        d7 = odf.set_index('ID(HTSCSecurityID)')
        d8 = pdf.set_index('ID(HTSCSecurityID)')
        d71 = d7.merge(odf2, how='inner', left_index=True, right_on='ID(HTSCSecurityID)',
                       suffixes=['_XX', '_YY']).to_pandas().drop(columns='index', errors='ignore')
        d71 = d71.sort_values(by=list(d71.columns))
        d81 = d8.merge(pdf2, how='inner', left_index=True, right_on='ID(HTSCSecurityID)',
                       suffixes=['_XX', '_YY'])
        d81 = d81.sort_values(by=list(d81.columns))
        d72 = d7.merge(odf2, how='outer', left_index=True, right_on='ID(HTSCSecurityID)',
                       suffixes=['_XX', '_YY']).to_pandas().drop(columns='index', errors='ignore').fillna(value=0)
        d72 = d72.sort_values(by=list(d72.columns))
        d82 = d8.merge(pdf2, how='outer', left_index=True, right_on='ID(HTSCSecurityID)',
                       suffixes=['_XX', '_YY']).fillna(value=0)
        d82 = d82.sort_values(by=list(d82.columns))
        d73 = d7.merge(odf2, how='left', left_index=True, right_on='ID(HTSCSecurityID)',
                       suffixes=['_XX', '_YY']).to_pandas().drop(columns='index', errors='ignore').fillna(value=0)
        d73 = d73.sort_values(by=list(d73.columns))
        d83 = d8.merge(pdf2, how='left', left_index=True, right_on='ID(HTSCSecurityID)',
                       suffixes=['_XX', '_YY']).fillna(value=0)
        d83 = d83.sort_values(by=list(d83.columns))
        d74 = d7.merge(odf2, how='right', left_index=True, right_on='ID(HTSCSecurityID)',
                       suffixes=['_XX', '_YY']).to_pandas().drop(columns='index', errors='ignore').fillna(value=0)
        d74 = d74.sort_values(by=list(d74.columns))
        d84 = d8.merge(pdf2, how='right', left_index=True, right_on='ID(HTSCSecurityID)',
                       suffixes=['_XX', '_YY']).fillna(value=0)
        d84 = d84.sort_values(by=list(d84.columns))

        assert compare_df(d1.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d2.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d11.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d21.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d12.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d22.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d13.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'MDDate', 'id(OrderIndex)', 'MDTime', 'OrderQty',
                'OrderPrice', 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price',
                'Time']).fillna(value=0),
                          d23.sort_values(by=['ID(HTSCSecurityID)', 'MDDate', 'id(OrderIndex)', 'MDTime', 'OrderQty',
                                             'OrderPrice', 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price',
                                             'Time']).fillna(
                             value=0))

        assert compare_df(d31.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d41.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d32.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d42.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d33.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d43.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d34.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'id(OrderIndex)', 'MDTime', 'OrderQty',
                'OrderPrice', 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price',
                'Time']).fillna(value=0),
                          d44.sort_values(by=['ID(HTSCSecurityID)', 'id(OrderIndex)', 'MDTime', 'OrderQty',
                                             'OrderPrice', 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price',
                                             'Time']).fillna(
                             value=0))

        assert compare_df(d51.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d61.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d52.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d62.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d53.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(value=0),
                          d63.sort_values(by=['ID(HTSCSecurityID)', 'WeightedAvgBidPx', 'WeightedAvgOfferPx']).fillna(
                             value=0))
        assert compare_df(d54.to_pandas().drop(columns='index', errors='ignore').sort_values(
            by=['ID(HTSCSecurityID)', 'id(OrderIndex)', 'MDTime', 'OrderQty',
                'OrderPrice', 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price',
                'Time']).fillna(value=0),
                          d64.sort_values(by=['ID(HTSCSecurityID)', 'id(OrderIndex)', 'MDTime', 'OrderQty',
                                             'OrderPrice', 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price',
                                             'Time']).fillna(
                             value=0))

        assert compare_df(d71, d81)
        assert compare_df(d72, d82)
        assert compare_df(d73, d83)
        assert compare_df(d74, d84)

    def test_drop(self):
        d1 = odf.drop(columns=['MDDate', 'MDTime'])
        d2 = pdf.drop(columns=['MDDate', 'MDTime'])
        d3 = odf.drop(index=[0, 1])
        d4 = pdf.drop(index=[0, 1])
        d5 = odf.drop(labels=[0, 1], axis=0)
        d6 = pdf.drop(labels=[0, 1], axis=0)
        d7 = odf.drop(labels=['MDDate', 'xxxxx'], axis=1, errors='ignore')
        d8 = pdf.drop(labels=['MDDate', 'xxxxx'], axis=1, errors='ignore')

        assert compare_df(d1.to_pandas().drop(columns='index', errors='ignore'), d2, index='ID(HTSCSecurityID)')
        assert compare_df(d3.to_pandas().drop(columns='index', errors='ignore'), d4, index='ID(HTSCSecurityID)')
        assert compare_df(d5.to_pandas().drop(columns='index', errors='ignore'), d6, index='ID(HTSCSecurityID)')
        assert compare_df(d7.to_pandas().drop(columns='index', errors='ignore'), d8, index='ID(HTSCSecurityID)')

    def test_drop_duplicates(self):
        d1 = odf.drop_duplicates(subset='ID(HTSCSecurityID)').to_pandas().drop(columns='index', errors='ignore')
        d1 = d1.sort_values(by=list(d1.columns))
        d2 = pdf.drop_duplicates(subset='ID(HTSCSecurityID)')
        d2 = d2.sort_values(by=list(d2.columns))
        assert compare_df(d1, d2)

    def test_filter(self):
        d1 = odf.filter(like='MD', axis=1).to_pandas().drop(columns='index', errors='ignore')
        d2 = pdf.filter(like='MD', axis=1)

        d3 = odf[odf['ID(HTSCSecurityID)'] == odf['id(OrderIndex)']].to_pandas().drop(columns='index', errors='ignore')
        d4 = pdf[pdf['ID(HTSCSecurityID)'] == pdf['id(OrderIndex)']]

        d5 = odf.filter(items=['MDDate', 'MDTime'], axis=1).to_pandas().drop(columns='index', errors='ignore')
        d6 = pdf.filter(items=['MDDate', 'MDTime'], axis=1)

        d7 = odf.filter(items=[0, 1], axis=0).to_pandas().drop(columns='index', errors='ignore')
        d8 = pdf.filter(items=[0, 1], axis=0)

        d9 = odf.filter(regex='e$', axis=1).to_pandas().drop(columns='index', errors='ignore')
        d10 = pdf.filter(regex='e$', axis=1)

        d11 = odf[odf['ID(HTSCSecurityID)'] >= "20170817"].to_pandas().drop(columns='index', errors='ignore')
        d12 = pdf[pdf['ID(HTSCSecurityID)'] >= "20170817"]

        d13 = odf[odf['OrderPrice'] * odf['OrderQty'] >= 4000].to_pandas().drop(columns='index', errors='ignore')
        d14 = pdf[pdf['OrderPrice'] * pdf['OrderQty'] >= 4000]

        assert compare_df(d1, d2)
        assert compare_df(d3, d4)
        assert compare_df(d5, d6)
        assert compare_df(d7, d8)
        assert compare_df(d9, d10)
        assert compare_df(d11, d12)
        assert compare_df(d13, d14)

    def test_astype(self):
        d1 = odf.astype({'MDDate': 'str', 'MDTime': "str"})
        assert isinstance(d1.loc[0, 'MDDate'], str)

    def test_set_index(self):
        d1 = odf.set_index('MDDate')
        assert d1.index.name == 'MDDate'

    def test_groupby(self):
        d1 = odf.groupby(by=['MDDate', 'MDTime']).mean().to_pandas().drop(
            columns=['MDDate', 'MDTime'], errors='ignore').as_matrix().tolist()
        d2 = pdf.groupby(by=['MDDate', 'MDTime']).mean().as_matrix().tolist()
        d3 = odf.groupby(by=['MDDate', 'MDTime']).max().to_pandas().drop(
            columns=['MDDate', 'MDTime'], errors='ignore')
        d4 = pdf.groupby(by=['MDDate', 'MDTime']).max()
        d5 = odf.groupby(by=['MDDate', 'MDTime']).min().to_pandas().drop(
            columns=['MDDate', 'MDTime'], errors='ignore')
        d5 = pd.DataFrame(data=d5.as_matrix().tolist(), columns=d5.columns).sort_values(
            by=['ID(HTSCSecurityID)', 'OrderQty', 'OrderPrice', 'id(OrderIndex)']).reset_index(drop=True)
        d6 = pdf.groupby(by=['MDDate', 'MDTime']).min()
        d6 = pd.DataFrame(data=d6.as_matrix().tolist(), columns=d6.columns).sort_values(
            by=['ID(HTSCSecurityID)', 'OrderQty', 'OrderPrice', 'id(OrderIndex)']).reset_index(drop=True)
        d7 = odf.groupby(by=['MDDate', 'MDTime']).sum().to_pandas().drop(
            columns=['MDDate', 'MDTime'], errors='ignore').as_matrix().tolist()
        d8 = pdf.groupby(by=['MDDate', 'MDTime']).sum().as_matrix().tolist()
        assert d1 == d2
        assert compare_df(d3, d4)
        assert compare_df(d5, d6)
        assert d7 == d8

    def test_map(self):
        def add(a, b):
            return a+b
        d1 = list(map(add, odf['MDDate'], odf['OrderPrice']))
        d2 = list(map(add, pdf['MDDate'], pdf['OrderPrice']))
        assert d1 == d2

    def test_sort_values(self):
        d1 = odf.sort_values(by=['id(OrderIndex)', 'OrderQty'], ascending=True).to_pandas().drop(columns='index', errors='ignore')
        d2 = pdf.sort_values(by=['id(OrderIndex)', 'OrderQty'], ascending=True)
        assert compare_df(d1, d2)

    def test_rename(self):
        d2 = pdf.rename(columns={'MDDate': 'Date'})
        d1 = odf.rename(columns={'MDDate': 'Date'}).to_pandas().drop(columns='index', errors='ignore')
        assert compare_df(d1, d2)

    def test_join(self):
        d1 = odf.join(other=odf2, how='inner', lsuffix='_XX', rsuffix='_YY').to_pandas().drop(columns='index', errors='ignore')
        d2 = pdf.join(other=pdf2, how='inner', lsuffix='_XX', rsuffix='_YY')
        d3 = odf.join(other=odf2, how='outer', lsuffix='_XX', rsuffix='_YY').to_pandas().drop(columns='index', errors='ignore').fillna(value=0)
        d4 = pdf.join(other=pdf2, how='outer', lsuffix='_XX', rsuffix='_YY').fillna(value=0)
        d5 = odf.join(other=odf2, how='outer', lsuffix='_XX', rsuffix='_YY').to_pandas().drop(columns='index', errors='ignore').fillna(
            value=0)
        d6 = pdf.join(other=pdf2, how='outer', lsuffix='_XX', rsuffix='_YY').fillna(value=0)
        d9 = odf.join(other=odf2, how='outer', lsuffix='_XX', rsuffix='_YY').to_pandas().drop(columns='index', errors='ignore').fillna(
            value=0)
        d10 = pdf.join(other=pdf2, how='outer', lsuffix='_XX', rsuffix='_YY').fillna(value=0)

        d7 = odf2.set_index('ID(HTSCSecurityID)')
        d8 = pdf2.set_index('ID(HTSCSecurityID)')
        d11 = odf.join(other=d7, how='inner', on='ID(HTSCSecurityID)', lsuffix='_XX', rsuffix='_YY').to_pandas().drop(
            columns='index', errors='ignore').fillna(value=0).sort_values(
            by=["MDTime", "OrderQty", "OrderPrice", 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price'])
        d12 = pdf.join(other=d8, how='inner', on='ID(HTSCSecurityID)', lsuffix='_XX', rsuffix='_YY').fillna(
            value=0).sort_values(
            by=["MDTime", "OrderQty", "OrderPrice", 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price'])
        d13 = odf.join(other=d7, how='outer', on='ID(HTSCSecurityID)', lsuffix='_XX', rsuffix='_YY').to_pandas().drop(
            columns='index', errors='ignore').fillna(value=0).sort_values(
            by=["MDTime", "OrderQty", "OrderPrice", 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price'])
        d14 = pdf.join(other=d8, how='outer', on='ID(HTSCSecurityID)', lsuffix='_XX', rsuffix='_YY').fillna(
            value=0).sort_values(
            by=["MDTime", "OrderQty", "OrderPrice", 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price'])
        d15 = odf.join(other=d7, how='left', on='ID(HTSCSecurityID)', lsuffix='_XX', rsuffix='_YY').to_pandas().drop(
            columns='index', errors='ignore').fillna(value=0).sort_values(
            by=["MDTime", "OrderQty", "OrderPrice", 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price'])
        d16 = pdf.join(other=d8, how='left', on='ID(HTSCSecurityID)', lsuffix='_XX', rsuffix='_YY').fillna(
            value=0).sort_values(
            by=["MDTime", "OrderQty", "OrderPrice", 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price'])
        d17 = odf.join(other=d7, how='right', on='ID(HTSCSecurityID)', lsuffix='_XX', rsuffix='_YY').to_pandas().drop(
            columns='index', errors='ignore').fillna(value=0).sort_values(
            by=["MDTime", "OrderQty", "OrderPrice", 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price'])
        d18 = pdf.join(other=d8, how='right', on='ID(HTSCSecurityID)', lsuffix='_XX', rsuffix='_YY').fillna(
            value=0).sort_values(
            by=["MDTime", "OrderQty", "OrderPrice", 'WeightedAvgBidPx', 'WeightedAvgOfferPx', 'Sell1Price'])

        assert compare_df(d1, d2)
        assert compare_df(d3, d4)
        assert compare_df(d5, d6)
        assert compare_df(d9, d10)
        assert compare_df(d11, d12)
        assert compare_df(d13, d14)
        assert compare_df(d15, d16)
        assert compare_df(d17, d18)

    def test_series_op(self):
        d1 = (odf['OrderQty'] >= 1600).tolist()
        d2 = (pdf['OrderQty'] >= 1600).tolist()
        assert d1 == d2

    def test_series_loc(self):
        d1 = odf['MDDate'].loc[0:4].tolist()
        d2 = pdf['MDDate'].loc[0:4].tolist()

        d3 = odf.loc[2].loc['MDDate':'MDTime'].tolist()
        d4 = pdf.loc[2].loc['MDDate':'MDTime'].tolist()

        assert d1 == d2
        assert d3 == d4

    def test_series_iloc(self):
        d1 = odf['MDDate'].iloc[0:4].tolist()
        d2 = pdf['MDDate'].iloc[0:4].tolist()

        d3 = odf.loc[2].iloc[2:4].tolist()
        d4 = pdf.loc[2].iloc[2:4].tolist()

        assert d1 == d2
        assert d3 == d4

    def test_series_bracket(self):
        d1 = odf['MDDate'][0:4].tolist()
        d2 = pdf['MDDate'][0:4].tolist()

        d3 = odf.loc[2]['MDDate'].tolist()
        d4 = pdf.loc[2]['MDDate'].tolist()

        assert d1 == d2
        assert d3 == d4

    def test_loc_assign(self):
        d1 = odf
        d1.loc[0] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        d2 = pdf
        d2.loc[0] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        assert compare_df(d1, d2)

        d1 = odf
        d1.loc[[0, 2]] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        d2 = pdf
        d2.loc[[0, 2]] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        assert compare_df(d1, d2)

        d1 = odf
        d1.loc[[0, 2]] = [["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3],
                         ["000044.SZ", 20170810, "000044.SZ", "9:00", 2000, 2.0]]
        d2 = pdf
        d2.loc[[0, 2]] = [["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3],
                         ["000044.SZ", 20170810, "000044.SZ", "9:00", 2000, 2.0]]
        assert compare_df(d1, d2)

        d1 = odf
        d1.loc[0:1] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        d2 = pdf
        d2.loc[0:1] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        assert compare_df(d1, d2)

        d1 = odf
        d1.loc[0:1] = [["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3],
                          ["000044.SZ", 20170810, "000044.SZ", "9:00", 2000, 2.0]]
        d2 = pdf
        d2.loc[0:1] = [["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3],
                          ["000044.SZ", 20170810, "000044.SZ", "9:00", 2000, 2.0]]
        assert compare_df(d1, d2)

    def test_iloc_assign(self):
        d1 = odf
        d1.iloc[0] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        d2 = pdf
        d2.iloc[0] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        assert compare_df(d1, d2)

        d1 = odf
        d1.iloc[0:2] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        d2 = pdf
        d2.iloc[0:2] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        assert compare_df(d1, d2)

        d1 = odf
        d1.iloc[0:2] = [["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3],
                          ["000044.SZ", 20170810, "000044.SZ", "9:00", 2000, 2.0]]
        d2 = pdf
        d2.iloc[0:2] = [["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3],
                          ["000044.SZ", 20170810, "000044.SZ", "9:00", 2000, 2.0]]
        assert compare_df(d1, d2)

        d1 = odf
        d1.iloc[[0, 2]] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        d2 = pdf
        d2.iloc[[0, 2]] = ["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3]
        assert compare_df(d1, d2)

        d1 = odf
        d1.iloc[[0, 2]] = [["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3],
                          ["000044.SZ", 20170810, "000044.SZ", "9:00", 2000, 2.0]]
        d2 = pdf
        d2.iloc[[0, 2]] = [["000045.SZ", 20170816, "000045.SZ", "9:10", 2300, 2.3],
                          ["000044.SZ", 20170810, "000044.SZ", "9:00", 2000, 2.0]]
        assert compare_df(d1, d2)

    def test_frame_assign(self):
        d1 = odf
        d1['MDDate'] = "20180101"
        d2 = pdf
        d2['MDDate'] = "20180101"
        assert compare_df(d1, d2)

        d1 = odf
        d1['MDDate'] = ["20180101", "20180102", "20180103", "20180104", "20180105",
                        "20180106", "20180107", "20180108", "20180109", "20180110"]
        d2 = pdf
        d2['MDDate'] = ["20180101", "20180102", "20180103", "20180104", "20180105",
                        "20180106", "20180107", "20180108", "20180109", "20180110"]
        assert compare_df(d1, d2)

        d1 = odf
        d1[['MDDate', 'MDTime']] = "20180101"
        d2 = pdf
        d2[['MDDate', 'MDTime']] = "20180101"
        assert compare_df(d1, d2)

        d1 = odf
        d1[['MDDate', 'MDTime']] = [["20180101", "20180101"], ["20180102", "20180102"], ["20180103", "20180103"],
                                    ["20180104", "20180104"],
                                    ["20180105", "20180105"], ["20180106", "20180106"], ["20180107", "20180107"],
                                    ["20180108", "20180108"],
                                    ["20180109", "20180109"], ["20180110", "20180110"]]
        d2 = pdf
        d2[['MDDate', 'MDTime']] = [["20180101", "20180101"], ["20180102", "20180102"], ["20180103", "20180103"],
                                    ["20180104", "20180104"],
                                    ["20180105", "20180105"], ["20180106", "20180106"], ["20180107", "20180107"],
                                    ["20180108", "20180108"],
                                    ["20180109", "20180109"], ["20180110", "20180110"]]
        assert compare_df(d1, d2)
