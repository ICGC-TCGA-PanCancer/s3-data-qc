import sys
import os
import json
import xmltodict
import time
import subprocess
import calendar
from ..job_tracker import move_to_next_step, get_job_json, save_job_json


name = 'downloading'
next_step = 'slicing'
s3_bucket_url = 's3://oicr.icgc/data/'

def get_name():
    global name
    return name


def download_bam_and_get_info(job_dir, object_id, file_name):
    global s3_bucket_url

    command =   'cd {} && '.format(job_dir) + \
                'aws s3 cp ' + s3_bucket_url + \
                object_id + ' ' + \
                file_name

    process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    out, err = process.communicate()
    if process.returncode:
        # should not exit for just this error, improve it later
        sys.exit('Unable to download xml file.\nError message: {}'.format(err))
    else:
        file_info = {
            'file_size': None,
            'file_md5sum': None
        }
        # file is here: os.path.join(job_dir, file_name)

        return file_info


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
    move_to_next_step(job, next_step)
    return True
