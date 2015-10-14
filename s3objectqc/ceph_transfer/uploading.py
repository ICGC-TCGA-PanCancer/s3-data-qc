import sys
import os
import json
import xmltodict
import time
import subprocess
import calendar
import hashlib
from ..job_tracker import move_to_next_step, get_job_json, save_job_json


name = 'uploading'
next_step = None
ceph_bucket_url = 's3://oicr.icgc/data/'

def get_name():
    global name
    return name

def upload_job(job):
    job_dir = job.job_dir
    for f in job.job_json.get('files'):
        object_id = f.get('object_id')
        file_name = f.get('file_name')
        ftype = file_name.split('.')[-1]
        file_info = upload_file(job_dir, file_name, object_id)
        if file_info.get('upload_time') is not None:
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                ftype + '-upload_time': file_info.get('upload_time')
            })
        else:
            return False

    return True


def upload_file(job_dir, file_name, object_id):
    file_info = {}
    fpath = os.path.join(job_dir, file_name)
    start_time = int(calendar.timegm(time.gmtime()))
    if not os.path.isfile(fpath):
        return
    else:
        pass
        # command =   'cd {} && '.format(job_dir) + \
        #             'aws --endpoint-url https://www.cancercollaboratory.org:9080 s3 cp ' + \
        #             file_name + ' ' + \
        #             ceph_bucket_url + object_id 

        # process = subprocess.Popen(
        #         command,
        #         shell=True,
        #         stdout=subprocess.PIPE,
        #         stderr=subprocess.PIPE
        #     )

        # out, err = process.communicate()

        # if process.returncode:
        #     return
            # should not exit for just this error, improve it later
            # sys.exit('Unable to upload file to ceph.\nError message: {}'.format(err))
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
        for f in job.job_json.get('files'):
            if not f.get('file_name').endswith('.bam'): continue   
            local_bam_file = os.path.join(job.job_dir, f.get('file_name'))
            # remove the HUGH bam file when match
            os.remove(local_bam_file)
            move_to_next_step(job, 'completed')
