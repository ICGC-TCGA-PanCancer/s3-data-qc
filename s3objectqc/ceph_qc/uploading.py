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
next_step = 'slicing'
bucket_url = 's3://oicr.icgc/data/'

def get_name():
    global name
    return name

def remove_remote_files(job):
    file_info = {}
    job_dir = job.job_dir
    start_time = int(calendar.timegm(time.gmtime()))
    for f in ['bai_file', 'bam_file', 'xml_file']:
        object_id = job.job_json.get(f).get('object_id')

        command =   'cd {} && '.format(job_dir) + \
                    'aws --endpoint-url https://www.cancercollaboratory.org:9080 s3 rm ' + bucket_url + object_id 
        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        out, err = process.communicate()        

        if process.returncode:
            return False

    end_time = int(calendar.timegm(time.gmtime()))
    file_info['delete_time'] = end_time - start_time

    job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
        'delete_time': file_info.get('delete_time')
    })
    return True


def need_to_upload(job):

    file_info = {}
    start_time = int(calendar.timegm(time.gmtime()))
    job_dir = job.job_dir
    for f in ['bai_file', 'bam_file', 'xml_file']:
        object_id = job.job_json.get(f).get('object_id')

        command =   'cd {} && '.format(job_dir) + \
                    'aws --endpoint-url https://www.cancercollaboratory.org:9080 s3 ls ' + bucket_url + \
                    object_id + ' |grep meta > ls.out'

        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        out, err = process.communicate()

        if process.returncode:
            file_info['error'] = 'failed to check the meta'
            return file_info
    
        with open(job_dir+'/ls.out', 'r') as f: out_str = f.read()
        if not '.meta' in out_str: 
            file_info['need_to_upload'] = True
            return file_info
    
    end_time = int(calendar.timegm(time.gmtime()))
    file_info['check_meta_time'] = end_time - start_time
    
    job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
        'check_meta_time': file_info.get('check_meta_time')
    })
    file_info['need_to_upload'] = False

    return file_info


def generate_manifest(job_dir, gnos_id, job_json):

    data_file_path = os.path.join(job_dir, gnos_id)
    xml_file = data_file_path + '.xml'
    if not os.path.isfile(xml_file) or not os.path.exists(data_file_path):
        return False   
    else:  # generate manifest file
        os.rename(xml_file, os.path.join(data_file_path, gnos_id + '.xml'))
        with open(os.path.join(job_dir, gnos_id + '.txt'), 'w') as m:
            for f in job_json.get('files'):
                object_id = f.get('object_id')
                file_name = f.get('file_name')
                m.write(object_id + '=' + os.path.join(job_dir, gnos_id, file_name) + '\n')

    return True


def upload_job(job):

    file_info = {}
    job_dir = job.job_dir
    gnos_id = job.job_json.get('gnos_id')
    start_time = int(calendar.timegm(time.gmtime()))
    if generate_manifest(job_dir, gnos_id, job.job_json):
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
            'upload_time': file_info.get('upload_time')
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

    file_info = need_to_upload(job)
    if file_info.get('error'): 
        move_to_next_step(job, 'failed')
    if file_info.get('need_to_upload') is True:
        # objects do not be successfully deleted
        if not remove_remote_files(job): move_to_next_step(job, 'failed')
        # file does not be successfully uploaded 
        if not upload_job(job): move_to_next_step(job, 'failed')
    move_to_next_step(job, next_step)
    local_file_dir = os.path.join(job.job_dir, job.job_json.get('gnos_id'))
    # remove the HUGH bam file when match
    if os.path.exists(local_file_dir): shutil.rmtree(local_file_dir, ignore_errors=True)
        
