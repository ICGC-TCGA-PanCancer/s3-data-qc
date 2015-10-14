import sys
import os
import json
import xmltodict
import time
import subprocess
import calendar
import hashlib
import shutil
from ..job_tracker import move_to_next_step, get_job_json, save_job_json


name = 'uploading'
next_step = None
ceph_bucket_url = 's3://oicr.icgc/data/'

def get_name():
    global name
    return name

def upload_job(job):
    job_dir = job.job_dir
    gnos_id = job.job_json.get('gnos_id')
    for f in job.job_json.get('files'):
        object_id = f.get('object_id')
        file_name = f.get('file_name')
        ftype = file_name.split('.')[-1]
        file_dir = job_dir if file_name.endswith('.xml') else os.path.join(job_dir, gnos_id)
        file_info = upload_file(file_dir, file_name, object_id)
        if file_info.get('upload_time') is not None:
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                ftype + '-upload_time': file_info.get('upload_time')
            })
        else:
            return False

    return True


def upload_file(file_dir, file_name, object_id):
    global ceph_bucket_url

    file_info = {}
    fpath = os.path.join(file_dir, file_name)
    start_time = int(calendar.timegm(time.gmtime()))
    if not os.path.isfile(fpath):
        return file_info
    else:
        command =   'cd {} && '.format(file_dir) + \
                    'aws --endpoint-url https://www.cancercollaboratory.org:9080 s3 cp ' + \
                    file_name + ' ' + \
                    ceph_bucket_url + object_id 
                    
        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        out, err = process.communicate()
        print err 
        if process.returncode:
            return file_info

    end_time = int(calendar.timegm(time.gmtime()))
    file_info['upload_time'] = end_time - start_time
    return file_info


def _start_task(job):
    job.job_json.get('_runs_').get(job.conf.get('run_id'))[get_name()] = {
        'start': int(calendar.timegm(time.gmtime()))
    }


def run(job):
    global name, next_step
    print ('running task: {}'.format(get_name()))

    _start_task(job)

    if not upload_job(job): # file does not be successfully uploaded 
        move_to_next_step(job, 'failed')
    else:
        move_to_next_step(job, 'completed')
        local_file_dir = os.path.join(job.job_dir, job.job_json.get('gnos_id'))
        # remove the HUGH bam file when match
        if os.path.exists(local_file_dir): shutil.rmtree(local_file_dir)
        
