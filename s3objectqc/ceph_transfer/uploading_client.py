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


name = 'uploading_client'
next_step = None
ceph_bucket_url = 's3://oicr.icgc/data/'

def get_name():
    global name
    return name


def generate_manifest(job_dir, gnos_id):

    xml_file = os.path.join(job_dir, gnos_id, '.xml')
    data_file_path = os.path.join(job_dir, gnos_id)
    if not os.path.isfile(xml_file) or not os.path.exists(data_file_path):
        return False   
    # generate manifest file
    else
        command =   'cd {} && '.format(job_dir) + \
                    'mv '
                    'dcc-metadata-client -i ' + gnos_id + ' -m ' + gnos_id + '.txt -o ./'
                    
        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        out, err = process.communicate()
        if process.returncode:
            return False
    return True  


def upload_job(job):

    file_info = {}
    job_dir = job.job_dir
    gnos_id = job.job_json.get('gnos_id')
    start_time = int(calendar.timegm(time.gmtime()))
    if generate_manifest(job):
        command =   'cd {} && '.format(job_dir) + \
                    'dcc-storage-client upload --manifest ' + gnos_id + '.txt'
                    
        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        out, err = process.communicate()
        if process.returncode:
            return file_info

        end_time = int(calendar.timegm(time.gmtime()))
        file_info['upload_time'] = end_time - start_time

    if file_info.get('upload_time') is not None:
        job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
            ftype + '-upload_time': file_info.get('upload_time')
        })
        return True
    else:
        return False
    


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
        if os.path.exists(local_file_dir): shutil.rmtree(local_file_dir, ignore_errors=True)
        
