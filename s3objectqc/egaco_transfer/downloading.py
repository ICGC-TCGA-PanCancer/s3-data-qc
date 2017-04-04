import sys
import os
import re
import json
import xmltodict
import time
import subprocess
import calendar
import hashlib
from ..job_tracker import move_to_next_step, get_job_json
from ..util import get_md5
from icgconnect import collab, utils, xml_audit


name = 'downloading'
next_step = 'uploading'
gnos_key = '/home/ubuntu/.ssh/gnos_key'


def get_name():
    global name
    return name


def _start_task(job):
    job.job_json.get('_runs_').get(job.conf.get('run_id'))[get_name()] = {
        'start': int(calendar.timegm(time.gmtime()))
    }


def run(job):
    global name, next_step
    print ('running task: {}'.format(get_name()))

    _start_task(job)
    print "downloading"

    # if everything was fine, finally move the job json file to the next_step folder
    move_to_next_step(job, next_step)
    return True
