import time


def main(expand=None, **kwargs):
    if expand is not None:
        print("expand value: ", expand)
        time.sleep(expand)
    else:
        print("kwargs value: ", kwargs)
        time.sleep(kwargs["time_sleep"])
    # time.sleep(expand)
    # print(kwargs['task_id'] + " is executed.")
