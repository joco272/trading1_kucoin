from datetime import datetime
import pandas as pd
from datetime import datetime, timedelta

pd.set_option('display.max_columns', None)
pd.set_option("expand_frame_repr", False)

date = datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M")
init = pd.DataFrame({'time': [date]})  # dummy datetime data to append as first row


cl = {"h": ["3847.72"], "l": ["3839.15"], "c": ["3847.72"], "vol": ["234612.90899491613"], "RSI": ["44.11158600211379"], "time": ["2021-09-04 19:16"], 'timeline': ['1']}
op = {"o": ["3877.02", '1'], "lower2": ["3859.270863778579", '1'], "upper2": ["3883.0071362214394", '1'], "lower1": ["3862.009664445063", '1'], "upper1": ["3880.2683355549552", '1'], "time": ["2021-09-04 19:15", "2021-09-04 19:20"], 'timeline': ['1', '1']}

data_var = {'time': None, 'timeline': float(), 'o': float(), 'h': float(), 'l': float(), 'c': float(),
            'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float(), 'vol': float(),
            'RSI': float(), 'SD_pct': float()}
date = datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M")
date2 = datetime.strptime("2021-09-04 19:16", "%Y-%m-%d %H:%M")

cls= pd.DataFrame.from_dict(cl)
opn = pd.DataFrame.from_dict(op)
data0 = pd.DataFrame(columns=data_var)
data0 = data0.append(init, ignore_index=True)

cls['time'] = pd.to_datetime(cls['time'], format="%Y-%m-%d %H:%M")
opn['time'] = pd.to_datetime(opn['time'], format="%Y-%m-%d %H:%M")

# opn = opn.set_index(['time'])
data0 = data0.set_index(['time'])
# cls = cls.set_index(['time'])

tl = int(cls.iloc[-1,6])
cls.iloc[-1,5] = cls.iloc[-1,5]-timedelta(minutes=tl)
# print('close tl:', tl)

print('open==========================:\n', opn)
print('close=========================:\n', cls)

# print(cls.loc[date2, 'RSI'])

data = pd.merge(left=opn, right=cls, how='inner', on=['time', 'timeline'])
data = data.set_index(['time'])
print('data=================================:\n', data)
# print('open shape:', opn.shape[0])

df_open_csv = opn[opn.shape[0]-1:opn.shape[0]]
# print('df_open_csv:\n', df_open_csv)

data0 = data0.append(data, ignore_index=False)
print('df_data0==================================:\n', data0)