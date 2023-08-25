import time
import pandas as pd
# from include.helpers import *

def task_extend2(expand=None, **kwargs):

    if expand is not None:
        print("expand value: ", expand)
        time.sleep(expand)   
    else:
        print("kwargs value: ", kwargs)
        time.sleep(kwargs['time_sleep'])
    # time.sleep(expand)   
    #print(kwargs['task_id'] + " is executed.")