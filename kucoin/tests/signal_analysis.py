import pandas as pd
from datetime import datetime, timedelta

pd.set_option('display.max_columns', None)
pd.set_option("expand_frame_repr", False)

data = pd.read_csv ('D:/Dropbox/Trading/James/csv/data/Sample8e.csv')
data['time'] = pd.to_datetime(data['time'], format="%m/%d/%Y %H:%M")
data['ts'] = pd.to_datetime(data['time'], format="%m/%d/%Y %H:%M")

# print(data)
data = data.set_index(['time', 'tl'])

data.sort_index(inplace=True)  # df needs to be sorted. Timestamp index slicing will not work otherwise
print(data)

x = data.loc[(slice('2021-10-14 20:30:00'), slice(5)),:]
print(x)