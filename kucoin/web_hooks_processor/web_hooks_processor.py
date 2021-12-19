import pandas as pd
import csv
import logging
import json
from datetime import datetime
from kucoin.analysis.analysis import Analysis

tk_var = {'time': '', 'price': float()}
logger = logging.getLogger()


class WebHookProcessor:
    def __int__(self):
        # self.content = content
        # Initialize Pandas df objects to run web_hooks_processor
        # op_var = {'o': float(), 'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float()}
        # self.df_data_open = pd.DataFrame(op_var, index=[])
        cl_var = {'h': float(), 'l': float(), 'c': float(), 'vol': float(), 'RSI': float()}
        self.df_data_close = pd.DataFrame(cl_var, index=[])
        tk_var = {'time': '', 'price': float()}
        self.df_ticker = pd.DataFrame(tk_var, index=[])


    def process_webhook_data(self, content, analysis_ob):
        # print('Data being processed:', content)
        path_csv = 'D:/Dropbox/Trading/James/csv/'
        csv_lst = []
        op_var = {'time': str(), 'timeline': float(), 'o': float(), 'lower2': float(), 'upper2': float(),
                  'lower1': float(), 'upper1': float()}
        cl_var = {'timeline': float(), 'time': str(), 'h': float(), 'l': float(), 'c': float(), 'vol': float(),
                  'RSI': float()}
        if content['title'] == 'open':
            df_data_open = pd.DataFrame(op_var, index=[])
            tl = content['timeline']
            o = content['data'][0]['o']
            u1 = content['data'][0]['upper1']
            l1 = content['data'][0]['lower1']
            u2 = content['data'][0]['upper2']
            l2 = content['data'][0]['lower2']
            t_to_process = content['data'][0]['time']
            t = t_to_process[0: -4].replace('T', ' ')
            # edit `t` string here
            append = {'time': [t], 'timeline': [int(tl)], 'o': [float(o)], 'lower2': [float(l2)], 'upper2': [float(u2)],
                      'lower1': [float(l1)], 'upper1': [float(u1)]}  # values to append to the df

            df_append = pd.DataFrame.from_dict(append)
            df_data_open = pd.merge(df_data_open, df_append, how="outer")

            #convert date to datetime format
            df_data_open['time'] = pd.to_datetime(df_data_open['time'], format="%Y-%m-%d %H:%M")
            # df_data_open = df_data_open.set_index(['time'])

            # print('===========================================================')
            # print(df_data_open)
            # print('===========================================================')
            # Insert code to send data to analysis
            analysis_ob.merge_open(df_data_open)

        elif content['title'] == 'close':
            df_data_close = pd.DataFrame(cl_var, index=[])
            tl = content['timeline']
            h = content['data'][0]['h']
            l = content['data'][0]['l']
            c = content['data'][0]['c']
            vol = content['data'][0]['vol']
            rsi = content['data'][0]['RSI']
            t_to_process = content['data'][0]['time']
            t = t_to_process[0: -4].replace('T', ' ')
            append = {'time': [t], 'timeline': [int(tl)], 'h': [float(h)], 'l': [float(l)], 'c': [float(c)],
                      'vol': [float(vol)], 'RSI': [float(rsi)]}
            df_append = pd.DataFrame.from_dict(append)
            df_data_close = pd.merge(df_data_close, df_append, how="outer")
            df_data_close['time'] = pd.to_datetime(df_data_close['time'], format="%Y-%m-%d %H:%M")
            # df_data_close = df_data_close.set_index(['time'])
            # print('===========================================================')
            # print(df_data_close)
            # print('===========================================================')
            # Insert code to send data to analysis
            analysis_ob.merge_close(df_data_close)
        else:
            # print('Data file is not Open or Close')
            logger.warning('Data file is not of Open or Close type')
        # Send to data frame

        # send to csv filr

        return
    # df's here need to be changed so index=time as in data
    def process_webhook_trade(self, content, analysis_ob):
        tr_var = {'time': str(), 'timeline': int(), 'transaction': str(), 'title': str()}
        df_data_transaction = pd.DataFrame(tr_var, index=[])

        if content['transaction'] in ['buy', 'sell']:
            tr = content['transaction']
            title = content['title']
            tl = content['timeline']
            t_to_process = content['time']
            t = t_to_process[0: -4].replace('T', ' ')
            append = {'time': [t], 'timeline': [int(tl)], 'transaction': [tr], 'title': [title]}
            df_append = pd.DataFrame.from_dict(append)
            df_data_transaction = pd.merge(df_data_transaction, df_append, how="outer")
            df_data_transaction['time'] = pd.to_datetime(df_data_transaction['time'], format="%Y-%m-%d %H:%M")
            # df_data_transaction = df_data_transaction.set_index(['time'])
            analysis_ob.merge_trade(df_data_transaction)
        else:
            print('Transaction is not BUY or SELL')
            logger.warning('Transaction is not BUY or SELL')
            print('Trade is being processed')
        return

    # df's here need to be changed so index=time as in data
    def process_webhook_action(self, content, analysis_ob):
        ac_var = {'time': str(), 'timeline': int(), 'b': int(), 's': int(), 'n': int(), 'c': int(), 'ts': str(), 'tl': int()}
        """Variables used: 'b = Buy; 's' = Sell, 'n' = None, 'c' = Complete
        'Time' = opening time of the candle as registered by TradingView.com
        'Timeline' = the time resolution of the candle   
        This webhook is received for every closing candle to indicate if a buy, or sell signal was received; or None
        The 'None' signal is a confirmation that it is working properly
        The 'complete' is always 0. It is marked as 1 when it is confirmed in analysis that action messages
        have been received for all corresponding sub-candles"""
        t_to_process = content['time']
        t = t_to_process[0: -4].replace('T', ' ')
        tl = int(content['timeline'])
        b = int(content['B'])
        s = int(content['S'])
        n = int(content['N'])
        c = 0
        append = {'time': [t], 'timeline': [tl], 'b': [b], 's': [s], 'n': [n], 'c': [c], 'ts': [t], 'tl': [tl]}
        df_action = pd.DataFrame.from_dict(append)
        df_action['time'] = pd.to_datetime(df_action['time'], format="%Y-%m-%d %H:%M")
        df_action['ts'] = pd.to_datetime(df_action['time'], format="%Y-%m-%d %H:%M")
        analysis_ob.merge_action(df_action)
        return