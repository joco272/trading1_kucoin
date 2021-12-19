import websocket
import logging
from kucoin.base_request.base_request import KucoinBaseRestApi
import json
import pandas as pd
import threading
from datetime import datetime
from time import sleep
logger = logging.getLogger()


class WebSocket2():
    def __init__(self):

        self.public_endpoint = '/api/v1/bullet-public'
        self.private_endpoint = '/api/v1/bullet-private'
        self.logger = logging.getLogger()

        self.ws = None

        self.sandbox_key = '6102dc5bbc85c200065a5cf7'
        self.sandbox_secret = '98f593a9-de00-4ae6-90f8-47d7f239773b '
        self.sandbox_passphrase = 'Dunedog99*'

        self.sb_sub1_key = '61031b0fbc85c200065a5d4b'
        self.sb_sub1_secret = '885767af-5871-41ca-ab3e-957c527cc4e6'
        self.sb_sub1_passphrase = 'Dunedog99*'
        self.sb_sub_user1 = ''

        self.main_key = '61046016ead9100006ee2c3c'
        self.main_secret = 'bca41fe7-bf77-42a2-a16c-b4f2b6a72ade'
        self.main_passphrase = 'Dunedog99*'

        self.sub1_key = '6106e528eadc7d000640f64d'
        self.sub1_secret = 'bf18297d-f270-4bce-a9ba-617d9f5131c2'
        self.sub1_passphrase = 'Dunedog99*'
        self.sub_user1 = '60c4e4491fb5e80006b22dff'

        # Base URLs:
        self.url = 'https://api.kucoin.com'
        self.sandbox_url = 'https://openapi-sandbox.kucoin.com'

        # Channel subscription ID
        self.id = 1

        #create a parallel thread for the WebSocket
        t = threading.Thread(target=self.start_ws)
        t.start()


    # Get the websocket token
    def get_ws_token(self, public=True):
        # https://docs.kucoin.com/#websocket-feed
        api = KucoinBaseRestApi(self.main_key, self.main_secret, self.main_passphrase, False, self.url, False)
        if public:
            tk = api._request('POST', '/api/v1/bullet-public')
        else:
            tk = api._request('POST', '/api/v1/bullet-private')

        token = tk['token']
        is_dict = tk['instanceServers'][0]
        ws_endpoint = is_dict['endpoint']
        ws_pingInterval = int(is_dict['pingInterval']) / 1000
        ws_pingTimeout = int(is_dict['pingTimeout']) / 1000

        connect_str = ws_endpoint + '?token=' + token

        print(f'Token: {tk}')
        print('ping interval:', ws_pingInterval)
        print('timeout:', ws_pingTimeout)

        return (connect_str, ws_pingTimeout, ws_pingInterval)

    # open the connection
    def start_ws(self, public=True):
        connect_str, ws_pingTimeout, ws_pingInterval = self.get_ws_token(public)
        self.ws = websocket.WebSocketApp(connect_str, on_open=self.on_open, on_close=self.on_close,
                                         on_error=self.on_error, on_message=self.on_message)

        self.ws.run_forever(ping_timeout=ws_pingTimeout, ping_interval=ws_pingInterval)
        return

    # ================================web socket actions========================================
    def on_open(self, ws):
        current_time = datetime.now()
        now = current_time.strftime("%H:%M:%S")
        logger.info('Kucoin Web Socket connection opened.')
        print(f'Kucoin Web Socket connection opened at {now}')
        self.subscribe_channel('ETH-USDT')
        return

    def on_close(self, ws):
        logger.warning(f'KuCoin Web Socket connection has closed.')

        return

    def on_error(self, ws, msg):
        current_time = datetime.now()
        now = current_time.strftime("%H:%M:%S")
        logger.error(f'KuCoin Web Socket connection error: {msg} at {now}')
        return

    def on_message(self, ws, msg):
        # Returns a str(dict())
        if msg.startswith('{"type":"message"'):
            self.process_ticker(msg)
        else:
            print(f'Other message: {msg}')
            print('Websocket connection is now working')
        return

    # ==================================Channel Subscriptions============================================
    def subscribe_channel(self, symbol):
        data = dict()

        data['id'] = self.id
        data['type'] = 'subscribe'
        data['topic'] = '/market/ticker:'+symbol
        data['privateChannel'] = 'false'
        data['response'] = 'true'

        self.ws.send(json.dumps(data))

        self.id += 1
        return

    # Process ticker info received with web socket
    def process_ticker(self, msg):
        d = json.loads(msg) # convert message to dict
        sequence = d['data']['sequence']
        time_stamp = d['data']['time']
        price = d['data']['price']
        print('Ticker ETH-USDT. Price:', price, '==Time:', time_stamp)

        # print(message)
        # time stamp is is milliseconds
        return
