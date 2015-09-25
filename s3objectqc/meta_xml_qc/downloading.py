import time
from ..job_tracker import move_to_next_step, get_job_json, save_job_json


name = 'downloading'
next_step = None

def get_name():
    global name
    return name


def get_file_size():
    pass


def download_file():
    pass


def run(conf, job_json_file):
    global name, next_step
    print ('running task: {}'.format(get_name()))

    # many steps here
    time.sleep(15)


    # if everything was fine, finally move the job json file to the next_step folder
    move_to_next_step(conf, name, 'match', job_json_file)
