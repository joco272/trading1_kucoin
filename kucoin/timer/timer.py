from time import time, sleep
from datetime import datetime
from threading import Thread

# This file can be deleted

class Timer(object):
    def __init__(self, analysis_ob):
        self.analysis_ob = analysis_ob
        self.ticker = '00:00:00'
        self.data_15 = '00:00:00'
        self.data_5 = '00:00:00'
        self.data_1 = '00:00:00'
        self.buy_15 = '00:00:00'
        self.buy_5 = '00:00:00'
        self.sell_15 = '00:00:00'
        self.sell_5 = '00:00:00'
        self.target = '00-00-00'

        # t = Thread(target=self.file_name_timer())
        # t.start()
        print('==============Timer is starting====================')

    def file_name_timer(self):
        while True:
            # print(self.ticker)
            sleep(1)
            current_time = datetime.now()
            now = current_time.strftime("%H-%M-%S")
            date = current_time.strftime("%Y_%m_%d")
            # print(f'now = {now} target = {self.target}')
            if now == self.target:
                self.analysis_ob.change_date_for_file(date)
                print('====================target reached=============================')
