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
data_bucket_url = 's3://oicr.icgc/data/'
meta_bucket_url = 's3://oicr.icgc.meta/metadata/'

def get_name():
    global name
    return name


def generate_manifest(job_dir, gnos_id, job_json):

    data_file_path = os.path.join(job_dir, gnos_id)
    xml_file = data_file_path + '.xml'
    if not os.path.isfile(xml_file) or not os.path.exists(data_file_path):
        return False   
    else:  # generate manifest file
        os.rename(xml_file, os.path.join(data_file_path, gnos_id + '.xml'))
        with open(os.path.join(job_dir, gnos_id + '.txt'), 'w') as m:
            m.write('\t'.join(['File UUID', 'File name', 'File md5sum']) + '\n')
            for f in job_json.get('files'):
                object_id = f.get('object_id')
                file_name = f.get('file_name')
                file_md5sum = f.get('file_md5sum')
                m.write('\t'.join([object_id, os.path.join(job_dir, gnos_id, file_name), file_md5sum]) + '\n')

    return True


def upload_job(job):

    file_info = {}
    job_dir = job.job_dir
    gnos_id = job.job_json.get('gnos_id')
    start_time = int(calendar.timegm(time.gmtime()))
    if generate_manifest(job_dir, gnos_id, job.job_json):
        command =   'cd {} && '.format(job_dir) + \
                    'icgc-storage-client --profile aws upload --force --manifest ' + gnos_id + '.txt'
                    
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
            'upload_time': file_info.get('upload_time')
        })
        return True
    else:
        return False

def copy_meta_file(job):
    file_info = {}
    for f in job.job_json.get('files'):
        if not f.get('file_name').endswith('.xml'): continue
        object_id = f.get('object_id')

    start_time = int(calendar.timegm(time.gmtime()))

    command = 'aws --profile amazon_pay s3 cp ' + \
                data_bucket_url + object_id + ' ' + meta_bucket_url + object_id
                
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
    file_info['copy_meta_time'] = end_time - start_time

    if file_info.get('copy_meta_time') is not None:
        job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
            'copy_meta_time': file_info.get('copy_meta_time')
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
        return False
    if not copy_meta_file(job):
        move_to_next_step(job, 'failed')
        return False

    local_file_dir = os.path.join(job.job_dir, job.job_json.get('gnos_id'))
    # remove the HUGH bam file when match
    if os.path.exists(local_file_dir): shutil.rmtree(local_file_dir, ignore_errors=True)
    move_to_next_step(job, 'completed')
    return True
        
