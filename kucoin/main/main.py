import tkinter as tk
import logging

from clients.kuCoin_client import KuCoinSpotClient

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s :: %(message)s')
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('info.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

if __name__ == '__main__':

    kc_client = KuCoinSpotClient(False)
    kc_client.get_sub_account()
    # get_sub_account()
    # print(get_accounts_info())
    # get_completed_orders()
    #pprint.pprint(TradeData(main_key, main_secret, main_passphrase).get_order_list())
    # print(TradeData(main_key, main_secret, main_passphrase).get_order_list())
    # print(create_limit_buy_order())
    # print(get_order_status())
    # print(TradeData(sub1_key, sub1_secret, sub1_passphrase).get_order_list())
    # print(create_market_order('sell', '123'))
    # get_sub_account()
    # pprint.pprint(UserData(main_key, main_secret, main_passphrase).get_sub_account(sub_user1))
    # print(cancel_all_orders())
    # pprint.pprint(UserData(sub1_key, sub1_secret, sub1_passphrase).get_account_ledger())
    # pprint.pprint(TradeData(sub1_key, sub1_secret, sub1_passphrase).get_order_list())
    root = tk.Tk()
    root.mainloop()