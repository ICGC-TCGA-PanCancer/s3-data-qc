import sys
import os
import json
import xmltodict
import time
import subprocess
import calendar
from ..job_tracker import move_to_next_step, get_job_json, save_job_json


name = 'slicing'
next_step = None
s3_bucket_url = 's3://oicr.icgc/data/'

def get_name():
    global name
    return name


def compare_file(job):
    return True


def _start_task(job):
    job.job_json.get('_runs_').get(job.conf.get('worker_id'))[get_name()] = {
        'start': int(calendar.timegm(time.gmtime()))
    }


def run(job):
    global name, next_step
    print ('running task: {}'.format(get_name()))

    _start_task(job)

    if not compare_file(job): # file does not match
        move_to_next_step(job, 'mismatch')
        return False

    # if everything was fine, finally move the job json file to the next_step folder
    move_to_next_step(job, 'match')
    return True
