import pandas as pd
import datetime

# Merging transactions together in one DF

pd.set_option('display.max_columns', None)
pd.set_option("expand_frame_repr", False)

tr_var = {'time': str(), 'timeline': int(), 'transaction': str()}
df_data_transaction = pd.DataFrame(tr_var, index=[])

mins = datetime.timedelta(minutes=5)
date = datetime.datetime.strptime('2013-10-12 4:5:00', "%Y-%m-%d %H:%M:%S")

content = {"type": "transaction","transaction": "sell","title": "BB-RSI","timeline": "5", "time":"2013-10-12T04:05:00Z"}
transactions = pd.DataFrame({'transaction': ['sell', 'sell', 'sell', 'sell', 'buy'], 'timeline': [5, 5, 5, 5, 15], 'time': ['2021-09-13 13:25','2021-09-13 13:30','2021-09-13 13:35', '2021-09-13 13:40', '2021-09-13 13:30']})
transactions['time'] = pd.to_datetime(transactions['time'], format="%Y-%m-%d %H:%M")
transactions = transactions.set_index(['time'])
# ======================================================================================================================

print('transactions\n', transactions)
print('=================================')

# ==============================if there is a sell signal at "timeline == 15"===========================================

# identify sell signals between the start and end of the 15m candle. To do that:
# (1) start has to be (alert time - timeline). In this case: 2021-09-13 13:45 - 15m = 2021-09-13 13:30
# (2) end has to be (alert time - 1m). If alert time is used
# new_trans = transactions.loc['2021-09-13 13:30':]#[transactions['timeline']==5]
# new_trans = transactions.loc[(transactions['timeline'] == 5) & (transactions['transaction'] == 'sell')]
new_trans = transactions.loc['2021-09-13 13:35':]
# print('New Trans:===================================\n', new_trans)

new_trans2 = transactions.loc['2021-09-13 13:30':'2021-09-13 13:34']
# new_trans2 = new_trans[(new_trans['transaction'] == 'sell')] #use when key = time + timeline
new_trans3 = transactions.loc['2021-09-13 13:30':][(transactions.loc['2021-09-13 13:30':]['timeline'] == 5) & (transactions.loc['2021-09-13 13:30':]['transaction'] == 'sell')]
new_trans3 = transactions.loc['2021-09-13 13:35':][(transactions.loc['2021-09-13 13:35':]['timeline'] == 5)]
# print('new_trans3\n', new_trans3)

"""trans_5_min_i=[]
for index, row in transactions[transactions['timeline']==5].iterrows():
    trans_5_min_i.append(row)"""


# last_row = new_trans3[new_trans3.shape[0]-1:]

# print('5 in trans:\n', trans_5_min_i)

# print('New Trans :\n', new_trans)
# print('last_row:\n', last_row)
# print("Shape 0 of New Trans 2:", new_trans2.shape[0])

"""index = new_trans2.index
index_list = list(index)

print(index_list)

for i in index_list:
    new_trans2 = new_trans2.drop(i)"""

"""ts = '2021-09-13 13:45:00'
ts = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")

print('New Trans 3:=====================\n', new_trans3)
print("Shape 0 of New Trans 3:", new_trans3.shape[0])

tl = new_trans.loc[ts, 'timeline']
print('timeline = ', tl)"""

print('New Trans 2 Before Drop:=====================\n', new_trans2)
# transactions = transactions.sort_index()
# print('Transactions Sorted:=====================\n', transactions)


if new_trans2.shape[0]>1:
    for j in list(set(list(new_trans2.index))):
        print('LIst Type', type(new_trans2.loc[j, 'timeline']))
        tl = list(new_trans2.loc[j, 'timeline'])
        print('tl type:', type(tl))
        for l in tl:
            temp = new_trans2.loc[j][(new_trans2.loc[j]['timeline'] == l)]
            print(f'{l} minute timeline:================\n', temp)
        new_trans2 = new_trans2.drop(j)

print('New Trans 2 After Drop:=====================\n', new_trans2)