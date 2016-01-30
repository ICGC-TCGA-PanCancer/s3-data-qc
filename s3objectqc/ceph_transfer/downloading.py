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
from random import randint


name = 'downloading'
next_step = 'uploading'
gnos_key = '/home/ubuntu/.ssh/gnos_key'


def get_name():
    global name
    return name


def download_metadata_xml(gnos_repo, gnos_id, job_dir, file_name):
    file_info = {
        'file_size': None,
        'file_md5sum': None
    }    

    fpath = os.path.join(job_dir, file_name)
    url = gnos_repo + 'cghub/metadata/analysisFull/' + gnos_id
    command =   'cd {} && '.format(job_dir) + \
                'wget ' + url + ' -O ' + file_name
    process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    out, err = process.communicate()
    if process.returncode:
        # should not exit for just this error, improve it later
        sys.exit('Unable to download metadata file from {}.\nError message: {}'.format(url, err))

    with open(fpath, 'r') as f: xml_str = f.read()

    data = re.sub(r'<ResultSet .+?>', '<ResultSet>', xml_str)

    with open(fpath + '.temp', 'w') as f:
        f.write(data)    

    # TODO: get file size and md5sum
    file_info['file_size'] = os.path.getsize(fpath + '.temp')
    os.remove(fpath + '.temp')
    file_info['file_md5sum'] = hashlib.md5(data).hexdigest()

    return file_info     


def download_datafiles(gnos_repo, gnos_id, job_dir, file_name):
    file_info = {}
    start_time = int(calendar.timegm(time.gmtime()))
    # Only download when file does not already exist.
    # - This is meant more for repeative testing/debugging without
    #   having to download large file over and over again.
    # - In real world, shouldn't have as each time a new run dir is created 
    url = gnos_repo + 'cghub/data/analysis/download/' + gnos_id
    for i in range(10):
        command =   'cd {} && '.format(job_dir) + \
                    'gtdownload -l gtdownload.log -c ' + gnos_key + ' ' + url

        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        out, err = process.communicate()

        if not process.returncode:
            file_info.pop('error', None)
            break

        file_info['error'] = 'gtdownload failed'
        time.sleep(randint(1,10))  # pause a few seconds before retry

    end_time = int(calendar.timegm(time.gmtime()))
    file_info['download_time'] = end_time - start_time
    return file_info


def compare_file(job):
    # we'd like to do
    # - download metadata file, check size and md5sum
    # - download data files if metadata file match
    job_dir = job.job_dir
    gnos_id = job.job_json.get('gnos_id')
    gnos_repo = job.job_json.get('gnos_repo')[0]

    for f in job.job_json.get('files'):
        if not f.get('file_name').endswith('.xml'): continue        
        file_name = f.get('file_name')
        file_info = download_metadata_xml(gnos_repo, gnos_id, job_dir, file_name)

        mismatch = False
        if not file_info.get('file_size') == f.get('file_size'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                file_name + '-size-mismatch': file_info.get('file_size')
            })
            mismatch = True

        if not file_info.get('file_md5sum') == f.get('file_md5sum'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                file_name + '-md5sum-mismatch': file_info.get('file_md5sum')
            })
            mismatch = True

        if mismatch: return False

    file_info = download_datafiles(gnos_repo, gnos_id, job_dir, file_name)
    job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
            'gnos-download_time': file_info.get('download_time')
        })
    if file_info.get('error'):
        job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
            'gtdownload-error': True
        })        
        return False

    return True


def _start_task(job):
    job.job_json.get('_runs_').get(job.conf.get('run_id'))[get_name()] = {
        'start': int(calendar.timegm(time.gmtime()))
    }


def run(job):
    global name, next_step
    print ('running task: {}'.format(get_name()))

    _start_task(job)

    if not compare_file(job): # file does not match
        move_to_next_step(job, 'failed')
        return False

    # if everything was fine, finally move the job json file to the next_step folder
    move_to_next_step(job, next_step)
    return True
