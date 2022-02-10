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
        tk_var = {'time': '', 'price': float()}
        self.df_ticker = pd.DataFrame(tk_var, index=[])


    def process_webhook_data(self, content, analysis_ob):
        # 2/7/22 Maybe I no longer need a function in the analysis object
        #++++++++++++++++++ I can append df rows from here?
        # +++++++++++++++++Maybe I can send from the flsk to the analysis object and skip the webhooks processor?
        # print('Data being processed:', content)
        path_csv = 'D:/Dropbox/Trading/James/csv/'
        csv_lst = []
        data_all_var = {'time': str(), 'timeline': float(), 's1': int(), 's2': int(), 'b1': int(), 'b2': int(), 'nt': int(),
                  'o': float(), 'h': float(), 'l': float(), 'c': float(), 'basis': float(), 'SD': float(),
                  'upper2': float(), 'upper1': float(), 'lower2': float(), 'lower1': float(), 'vol': float(), 'rsi': float()}

        if content['title'] == 'all':
            tl = content['timeline']
            o = content['data'][0]['o']
            h = content['data'][0]['o']
            l = content['data'][0]['o']
            c = content['data'][0]['o']
            u1 = content['data'][0]['upper1']
            l1 = content['data'][0]['lower1']
            u2 = content['data'][0]['upper2']
            l2 = content['data'][0]['lower2']
            basis = content['data'][0]['basis']
            sd =  content['data'][0]['SD']
            vol = content['data'][0]['vol']
            s1 = content['data'][0]['sell1']
            s2 = content['data'][0]['sell2']
            b1 = content['data'][0]['buy1']
            b2 = content['data'][0]['buy2']
            nt = content['data'][0]['nt']
            rsi = content['data'][0]['RSI']
            t_to_process = content['data'][0]['time']
            t = t_to_process[0: -4].replace('T', ' ')

            # values to append to the df
            append = {'time': [t], 'timeline': [int(tl)], 's1': [int(s1)], 's2': [int(s2)], 'b1': [int(b1)], 'b2': [int(b2)],
                      'nt': [int(nt)], 'o': [float(o)], 'h': [float(h)], 'l': [float(l)], 'c': [float(c)],
                      'basis': [float(basis)], 'SD': [float(sd)], 'lower2': [float(l2)], 'upper2': [float(u2)],
                      'lower1': [float(l1)], 'upper1': [float(u1)], 'vol': [float(vol)], 'rsi': [float(rsi)]}

            df_append = pd.DataFrame.from_dict(append)
            # df_data_open = pd.merge(df_data_open, df_append, how="outer")

            #convert date to datetime format
            df_append['time'] = pd.to_datetime(df_append['time'], format="%Y-%m-%d %H:%M")
            # df_data_open = df_data_open.set_index(['time'])

            # print('===========================================================')
            # print(df_data_open)
            # print('===========================================================')
            # Insert code to send data to analysis
            analysis_ob.merge_data_all(df_append)
        else:
            logger.warning('Data file is not of "ALL" type')
        return
