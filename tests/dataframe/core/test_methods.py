import pandas as pd
from configparser import ConfigParser
import os

from Octopus.dataframe.core import methods
from Octopus.dataframe.core.utils import compare_df

conf = ConfigParser()
user_home = os.path.expanduser('~')
conf_path = user_home + "/.config/octopus/config.ini"
conf.read(conf_path)

DATA_DIR = conf.get('test', 'DATA_DIR')

# path_pd = "../test_data/MDOrderRecord.csv"
# path_pd2 = "../test_data/MDStockRecord.csv"
import os
cur_dir = os.path.dirname(os.path.realpath(__file__))
path_pd = cur_dir + "/../test_data/MDOrderRecord.csv"
print(path_pd)
path_pd2 = cur_dir + "/../test_data/MDStockRecord.csv"
print(path_pd2)

pdf = pd.read_csv(path_pd)
pdf = pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True)
pdf2 = pd.read_csv(path_pd2)
pdf2 = pdf2.sort_values(by=list(pdf2.columns)).reset_index(drop=True)

odf = methods.from_pandas(pdf)
# df.to_csv(DATA_DIR + "/MDOrderRecord.csv")
# odf = methods.read_csv(DATA_DIR + "/MDOrderRecord.csv")

odf2 = methods.from_pandas(pdf2)
# df2.to_csv(DATA_DIR + "/MDStockRecord.csv")
# odf2 = methods.read_csv(DATA_DIR + "/MDStockRecord.csv")
# odf2 = odf2.sort_values(by=list(odf2.columns))


class TestMethods:

    def test_read_csv(self):
        pass





