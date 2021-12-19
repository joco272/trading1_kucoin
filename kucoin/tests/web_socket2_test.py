from kucoin.web_socket.web_socket2 import WebSocket2
from kucoin.flsk import flsk
from kucoin.analysis.analysis import Analysis
from kucoin.timer.timer import Timer
from threading import Thread
from multiprocessing import Process


if __name__ == "__main__":
    analysis = Analysis

    Process(target=Timer(analysis).file_name_timer).start()
    print('timer started')
    Process(target=WebSocket2().start_ws).start()
    print('WS started')
    Process(target=flsk.run_flsk, args=(analysis,)).start()
    print('Flask started')
