import pandas as pd
from datetime import datetime, timedelta
from csv import reader

pd.set_option('display.max_columns', None)
pd.set_option("expand_frame_repr", False)

data_var = {'time': None, 'timeline': float(), 'o': float(), 'h': float(), 'l': float(), 'c': float(),
            'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float(), 'vol': float(),
            'RSI': float(), 'SD_pct': float(), 'SD': float(), 'analyzed': str()}
data2_var = {'timeline': float(), 'o': float(), 'h': float(), 'l': float(), 'c': float(),
            'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float(), 'vol': float(),
            'RSI': float(), 'SD_pct': float(), 'SD': float(), 'analyzed': str()}
data2 = pd.DataFrame(columns=data2_var, index=[])

# cols = ['time', 'timeline', 'o', 'h', 'l', 'c', 'lower2', 'upper2', 'lower1', 'upper1', 'vol', 'RSI', 'SD_pct', 'SD']
# print(data2)
resolution = 15

with open('D:/Dropbox/Trading/James/csv/data/Sample2.csv', 'r') as read_obj:
    csv_reader = reader(read_obj)
    for row in csv_reader:
        dicti = {'time':[row[0]], 'timeline':[row[1]], 'o':[row[2]], 'h':[row[3]], 'l':[row[4]], 'c':[row[5]],
                'lower2':[row[6]], 'upper2':[row[7]], 'lower1':[row[8]], 'upper1':[row[9]], 'vol':[row[10]],
                'RSI':[row[11]], 'SD_pct':[row[12]], 'SD':[row[13]], 'analyzed':[row[14]]}
        df = pd.DataFrame.from_dict(dicti)
        df['time'] = pd.to_datetime(df['time'], format="%m/%d/%Y %H:%M")
        df['timeline'] = pd.to_numeric(df['timeline'])
        df['o'] = pd.to_numeric(df['o'])
        df['h'] = pd.to_numeric(df['h'])
        df['l'] = pd.to_numeric(df['l'])
        df['c'] = pd.to_numeric(df['c'])
        df['lower2'] = pd.to_numeric(df['lower2'])
        df['upper2'] = pd.to_numeric(df['upper2'])
        df['lower2'] = pd.to_numeric(df['lower2'])
        df['upper1'] = pd.to_numeric(df['upper1'])
        df['vol'] = pd.to_numeric(df['vol'])
        df['RSI'] = pd.to_numeric(df['RSI'])
        df['SD_pct'] = pd.to_numeric(df['SD'])
        df['SD_pct'] = pd.to_numeric(df['SD_pct'])
        df = df.set_index(['time'])
        data2 = data2.append(df,ignore_index=False)
        data2.sort_index(inplace=True)
        #the code until above mimics the receiving of data in the main dataframe
        #insert data here that will allow to identify signals in the various timelines
        temp_df = data2.iloc[-25:]
        id = data2.iloc[-1:].index

        temp_res_df = temp_df[temp_df['timeline']==resolution]
        # print('timeline for', id, 'is ', temp_df.loc[id]['timeline'], type(temp_df.loc[id]['timeline']))
        print(temp_res_df)
        if temp_res_df.shape[0]>0:
            temp_not_anal = temp_res_df[temp_res_df['analyzed']!='y']

            # print(temp_df)
            tl = temp_not_anal.iloc[-1][0]
            #print('============timeline is', tl, ' and type is', type(tl))
            # print('temp_not_anal\n', temp_df)
            if temp_not_anal.shape[0]>0:
                ###print(temp_not_anal.index)
                start = temp_not_anal.index
                end = temp_not_anal.index + timedelta(minutes=tl-1)
                if data2.loc[start:end].shape[0]>10:
                    ###print('located 5m version of 15m: ', start,'\n', data2.loc[start:end])
                    data2.loc[start]['analyzed'] = 'y'

print(data2)

# data2.sort_index(inplace=True)

#experimenting with slicing teh last items in the df and manipulating the indices to extract usable timestamp
print(data2)
idx = data2.iloc[-10:].index
sl = data2.iloc[-10:]
print('sl: +++++++++++++++++++\n',sl)
print('slice index: ', idx)

last_3 = {}

print('===================Indices comparison==========================')
for i in idx:
    print(type(i), i, end=', timeline: ')
    print(data2.loc[i]['timeline'], end=', Minutes: ')
    mins = i.minute
    print(mins," ", type(mins), end=", ")
    if mins % 5 == 0: # calculates the end of the slice
        delta = i + timedelta(minutes=mins - 1)
        print('delta =',delta)
    else:
        print('')

print(last_3)
