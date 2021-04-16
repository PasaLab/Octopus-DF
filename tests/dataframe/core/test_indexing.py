import pandas as pd
import numpy as np

from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
from Octopus.dataframe.core.sparkSeries import Series

path = '../test_data/MDOrderRecord.csv'
df = SparkDataFrame.from_csv(path)
#pyspark 默认读成str类型
pdf = pd.read_csv(path, dtype=str)

class TestIndexing:

    def test_loc_one_label(self):
        #test_df = np.array(df.loc[0]._to_pdata()).tolist()
        #test_pdf = np.array(pdf.loc[0]._to_pdata()).tolist()
        tested = df.loc[0]
        assert isinstance(tested, Series)

    def test_loc_one_label_with_cols(self):
        tested = df.loc[0, ['MDTime', 'MDDate']]
        assert isinstance(tested, Series)

    def test_loc_slice(self):
        tested = df.loc[0:2]
        assert isinstance(tested, SparkDataFrame)

    def test_loc_slice_with_cols(self):
        tested = df.loc[0:2, ['MDTime', 'MDDate']]
        assert isinstance(tested, SparkDataFrame)

    def test_loc_list(self):
        tested = df.loc[[0,2]]
        assert isinstance(tested, SparkDataFrame)

    def test_loc_list_with_cols(self):
        tested = df.loc[[0,2], ['MDTime', 'MDDate']]
        assert isinstance(tested, SparkDataFrame)

    def test_iloc_one_label(self):
        tested = df.iloc[0]
        assert isinstance(tested, Series)

    def test_iloc_list(self):
        tested = df.loc[[0, 2]]
        assert isinstance(tested, SparkDataFrame)

    def test_iloc_slice(self):
        tested = df.iloc[0:2]
        assert isinstance(tested, SparkDataFrame)

    def test_index_fetch_col(self):
        # one label
        tested = df['ID(HTSCSecurityID)']
        assert type(tested) == Series

    def test_index_fetch_dataframe(self):
        # one label
        tested1 = df[['ID(HTSCSecurityID)']]
        # multi labels
        tested2 = df[['ID(HTSCSecurityID)','MDTime']]
        assert type(tested1) == SparkDataFrame
        assert type(tested2) == SparkDataFrame

    #failed
    def test_loc_label_assign(self):
        tested = df
        tested.loc[2,["ID(HTSCSecurityID)"]] = 'newid'
        assert type(tested) == SparkDataFrame

    #failed
    def test_loc_label_assign2(self):
        tested = df
        tested.loc[2,"ID(HTSCSecurityID)"] = 'newid'
        assert type(tested) == SparkDataFrame

    def test_loc_slice_assign_multi_col(self):
        tested = df
        tested.loc[1:3, ["ID(HTSCSecurityID)", "MDTime"]] = [["shizi", 888], ["laohu", 8888], ["xiongmao", 88888]]
        assert type(tested) == SparkDataFrame

    #failed
    def test_loc_slice_assign_one_col(self):
        tested = df
        tested.loc[1:3, "ID(HTSCSecurityID)"] = [["shizi"],["laohu"],["xiongmao"]]
        assert type(tested) == SparkDataFrame

    def test_index_assign_multi_value(self):
        tested = df
        tested["ID(HTSCSecurityID)"] = ['newid1', 'newid2', 'newid3','2','3','4']
        assert type(tested) == SparkDataFrame

    #failed
    def test_index_assign_one_value(self):
        tested = df
        tested["ID(HTSCSecurityID)"] = 'newid1'
        assert type(tested) == SparkDataFrame
