import pandas as pd
from datetime import datetime

op_var = {'timeline': int(), 'time': str(), 'o': float(), 'lower2': float(), 'upper2': float(),
          'lower1': float(), 'upper1': float()}
cl_var = {'timeline': int(), 'time': str(), 'h': float(), 'l': float(), 'c': float(), 'vol': float(),
          'RSI': float(), 'SD_pct': float()}
data_var = {'time': None, 'timeline': float(), 'o': float(), 'h': float(), 'l': float(), 'c': float(),
            'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float(), 'vol': float(),
            'RSI': float(), 'SD_pct': float(), 'SD': float() }

date = datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M")
init = pd.DataFrame({'time': [date]})  # dummy datetime data to append as first row
init_data = pd.DataFrame({'time': [date],
                          'timeline': [0],
                          'o': ['o'],
                          'h': ['h'],
                          'l': ['l'],
                          'c': ['c'],
                          'lower2': ['u2'],
                          'upper2': ['l2'],
                          'lower1': ['l1'],
                          'upper1': ['u1'],
                          'vol': ['vol'],
                          'RSI': ['RSI'],
                          'SD_pct': ['SD Pct'],
                          'SD': ['SD']})

df_data = pd.DataFrame()
df_data = df_data.append(init_data, ignore_index=False)  # append dummy data as 1st row
df_data = df_data.set_index(['time'])  # set the index when df has 1st row w/dummy data. Ready to append rows

df_open = pd.read_csv ('D:/Dropbox/Trading/James/Sample Data Captured/2021_10_07_08_open.csv', names = op_var)
df_close = pd.read_csv ('D:/Dropbox/Trading/James/Sample Data Captured/2021_10_07_08_close.csv', names = cl_var)


pd.set_option('display.max_columns', None)

df_to_append = pd.merge(left=df_open, right=df_close, how='inner', on=['time', 'timeline'])
df_to_append.set_index(['time'])
df_to_append.drop(df_to_append.columns[1], axis=1)
for index, row in df_to_append.iterrows():
    upper1 = df_to_append.loc[index, 'upper1']
    lower1 = df_to_append.loc[index, 'lower1']
    close = df_to_append.loc[index, 'c']
    sd = (upper1 - lower1) / 2
    sd_pct = sd / close
    df_to_append.loc[index, 'SD_pct'] = sd_pct
    df_to_append.loc[index, 'SD'] = sd

# print(df_to_append)
df_data = df_data.append(df_to_append)
df_data.to_csv('D:/Dropbox/Trading/James/Sample Data Captured/2021_10_07_08_data.csv', index=True, mode='a', header=True)
df_close = df_close[:0]

df_data = df_data[df_data.shape[0]-10:]



# df_data.to_csv('D:/Dropbox/Trading/James/Sample Data Captured/2021_10_07_08_data.csv', index=True, mode='a', header=True)
# print('Size of DF Close:', df_close.shape[0])
print(df_data)