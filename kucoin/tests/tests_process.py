from multiprocessing import Process
from threading import Thread
import time

def func1(arg):
    while True:
        print(f"Func {arg} called.")
        time.sleep(1)

def func2(arg):
    while True:
        print(f"Func {arg} called.")
        time.sleep(1)

if __name__=='__main__':

    f1 = Process(target=func1, args=('1',))
    f2 = Process(target=func2, args=('2',))
    f1.start()
    f2.start()