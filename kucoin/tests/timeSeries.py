import pandas as pd
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option("expand_frame_repr", False)

data = pd.read_csv ('D:/Dropbox/Trading/James/csv/data/Sample.csv')
data['time'] = pd.to_datetime(data['time'], format="%m/%d/%Y %H:%M")

print(data)
data = data.set_index(['time'])

data.sort_index(inplace=True)  # df needs to be sorted. Timestamp index slicing will not work otherwise
il = list(data.index)
print(data)

dt2 = datetime(2021, 10, 14, 20, 30)
dt3 = datetime(2021, 10, 14, 20, 44)

sl = data.loc[dt2:dt3]
sz = sl.shape[0]
print("sl=============\n", sl)
print(type(sl)) # the slice will be of <class 'pandas.core.frame.DataFrame'> if 2+ results or <class 'pandas.core.series.Series'> if 1 result
print('size', sz)

sl5 = sl[sl['timeline']==5]
print(sl5)
print('SL5 size:', sl5.shape[0])

sl1 = sl[sl['timeline']==1]
print(sl1)
print('SL1 size:', sl1.shape[0])
# print(il)

