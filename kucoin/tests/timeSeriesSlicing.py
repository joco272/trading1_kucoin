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
counter = 0
with open('D:/Dropbox/Trading/James/csv/data/Sample2.csv', 'r') as read_obj:
    csv_reader = reader(read_obj)
    for row in csv_reader:
        counter += 1
        dicti = {'time':[row[0]], 'timeline':[row[1]], 'o':[row[2]], 'h':[row[3]], 'l':[row[4]], 'c':[row[5]],
                'lower2':[row[6]], 'upper2':[row[7]], 'lower1':[row[8]], 'upper1':[row[9]], 'vol':[row[10]],
                'RSI':[row[11]], 'SD_pct':[row[12]], 'SD':[row[13]], 'ts':[row[0]],'analyzed':[row[14]]}
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
        df['ts'] = pd.to_datetime(df['ts'], format="%m/%d/%Y %H:%M")
        df = df.set_index(['time'])
        data2 = data2.append(df,ignore_index=False)
        data2.sort_index(inplace=True)
        #the code until above mimics the receiving of data in the main dataframe
        #insert data here that will allow to identify signals in the various timelines
        temp_df = data2.iloc[-30:]
        id = data2.iloc[-30:].index

        temp_res_df = temp_df[temp_df['timeline']==resolution]
        print(f'counter is {counter}')
        # print (data2)
        if temp_res_df.shape[0]>0:
            temp_not_anal = temp_res_df[temp_res_df['analyzed']!='y']
            if temp_not_anal.shape[0] > 0:
                tl = temp_not_anal.iloc[-1][0]
                start = temp_not_anal.iloc[-1][14]
                end = start + timedelta(minutes=tl-1)

                dt2 = datetime(2021, 10, 14, 20, 15)
                dt3 = datetime(2021, 10, 14, 20, 29)

                try:
                    print(data2.loc[start:end])
                    if data2.loc[start:end].shape[0]>0:
                        print('located 5m version of 15m: ', start,'\n', data2.loc[start:end])
                        # temp_not_anal.loc[start, 'analyzed'] != 'y'
                        print(temp_not_anal)
                        data2[temp_not_anal.loc[start, ['analyzed'] != 'y']] = 'y'
                except:
                    print(f'invalid index for start: {start} and end: {end}, rows are ')
print(data2)
print(pd.__version__)