from datetime import datetime
import pandas as pd
from datetime import datetime, timedelta

# merging open and close signals into one DF

pd.set_option('display.max_columns', None)
pd.set_option("expand_frame_repr", False)


cl = {"h": ["3847.72"], "l": ["3839.15"], "c": ["3847.72"], "vol": ["234612.90899491613"], "RSI": ["44.11158600211379"], "time": ["2021-09-04 19:15"], 'timeline': ['1']}
op = {"o": ["3877.02"], "lower2": ["3859.270863778579"], "upper2": ["3883.0071362214394"], "lower1": ["3862.009664445063"], "upper1": ["3880.2683355549552"], "time": ["2021-09-04 19:15"], 'timeline': ['1']}
data_var = {'time': None, 'timeline': float(), 'o': float(), 'h': float(), 'l': float(), 'c': float(),
            'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float(), 'vol': float(),
            'RSI': float(), 'SD_pct': float()}
date = datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M")

data = pd.DataFrame({'time': [date], 'timeline': [None], 'o': [None], 'h': [None], 'l': [None], 'c': [None],
                     'lower2': [None], 'upper2': [None], 'lower1': [None], 'upper1': [None], 'vol': [None],
                     'RSI': [None], 'SD_pct': [None]})
data = data.set_index(['time'])
cls= pd.DataFrame.from_dict(cl)
opn = pd.DataFrame.from_dict(op)
cls['time'] = pd.to_datetime(cls['time'], format="%Y-%m-%d %H:%M")
opn['time'] = pd.to_datetime(opn['time'], format="%Y-%m-%d %H:%M")

opn = opn.set_index(['time'])###########################################################
print('df with dummy data=======================================')
print(data)


data = data.append(opn, ignore_index=False)#############################################

print('Data with Open appended==========================================')
print(data)

# add the close data
dt = cls.iloc[-1, 5] # the date that will be used as the name for the row where the `close` values will be inserted
td = int(cls.iloc[-1, 6])
mins = timedelta(minutes=td)

data.loc[dt-mins, 'h'] = cls.iloc[-1, 0] # data.locateByName[rowName, colName] = close.locateByIndex[lastRow, columnIndex]
data.loc[dt-mins, 'l'] = cls.iloc[-1, 1]
data.loc[dt-mins, 'c'] = cls.iloc[-1, 2]
data.loc[dt-mins, 'RSI'] = cls.iloc[-1, 3]
data.loc[dt-mins, 'vol'] = cls.iloc[-1, 4]

print('Close Data to insert============================================')
print(cls)

print('Data with Close============================================')
print(data)
print((data.loc[dt-mins, 'SD_pct'])==0)

dt2 = dt-mins
print('dt2: ', dt2)

df_1_csv = data[dt-mins:dt-mins]
print('Data Frame CSV:====================================================')
print(df_1_csv)