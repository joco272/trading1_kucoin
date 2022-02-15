import csv
import pandas as pd
from time import sleep

def read_historical_data(analysis):


    ollu_data = 'C:/Users/jocox/Dropbox/Trading/James/Historical_Data_for_Simulations/all_data.csv'
    envy_data = 'D:/Dropbox/Trading/James/Historical_Data_for_Simulations/all_data.csv'
    test_data = 'D:/Dropbox/Trading/James/Historical_Data_for_Simulations/test_all_data.csv'

    with open(envy_data, 'r') as data:
        reader = csv.reader(data)
        next(reader)
        for line in reader:
            # sleep(0.25)
            tl = float(line[1])
            o = float(line[2])
            h = float(line[3])
            l = float(line[4])
            c = float(line[5])
            u1 = float(line[11])
            l1 = float(line[12])
            u2 = float(line[9])
            l2 = float(line[10])
            basis = float(line[8])
            sd = float(line[19])
            vol = float(line[4])
            s1 = float(line[14])
            s2 = float(line[16])
            b1 = float(line[13])
            b2 = float(line[15])
            nt = float(line[17])
            rsi = float(line[6])
            t_to_process = line[0]
            t = t_to_process[0: -4].replace('T', ' ')

            append = {'time': [t], 'timeline': [int(tl)], 's1': [int(s1)], 's2': [int(s2)], 'b1': [int(b1)],
                      'b2': [int(b2)], 'nt': [int(nt)], 'o': [float(o)], 'h': [float(h)], 'l': [float(l)], 'c': [float(c)],
                      'basis': [float(basis)], 'SD': [float(sd)], 'lower2': [float(l2)], 'upper2': [float(u2)],
                      'lower1': [float(l1)], 'upper1': [float(u1)], 'vol': [float(vol)], 'rsi': [float(rsi)]}

            df_append = pd.DataFrame.from_dict(append)
            # df_data_open = pd.merge(df_data_open, df_append, how="outer")

            # convert date to datetime format
            df_append['time'] = pd.to_datetime(df_append['time'], format="%Y-%m-%d %H:%M")
            # df_data_open = df_data_open.set_index(['time'])

            analysis.merge_data_all(df_append)