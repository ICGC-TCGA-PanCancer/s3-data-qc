import sys
import os
import json
import xmltodict
import time
import subprocess
import calendar
from ..job_tracker import move_to_next_step, get_job_json
from ..util import get_md5


name = 'downloading'
next_step = 'slicing'
s3_bucket_url = 's3://oicr.icgc/data/'


def get_name():
    global name
    return name


def download_file_and_get_info(job_dir, object_id, file_name):
    global s3_bucket_url

    file_info = {
        'file_size': None,
        'file_md5sum': None
    }

    fpath = os.path.join(job_dir, file_name)

    start_time = int(calendar.timegm(time.gmtime()))

    # Only download when file does not already exist.
    # - This is meant more for repeative testing/debugging without
    #   having to download large file over and over again.
    # - In real world, shouldn't have as each time a new run dir is created 
    if not os.path.isfile(fpath):
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
            sys.exit('Unable to download file from s3.\nError message: {}'.format(err))

    end_time = int(calendar.timegm(time.gmtime()))

    file_info['download_time'] = end_time - start_time

    file_info['file_size'] = os.path.getsize(fpath)

    # run a quick check here to see how EOF is missing
    if file_name.endswith('.bam'):
        file_info['eof_missing'] = is_eof_missing(fpath)
        return file_info  # if eof missing, no need to continue

    start_time = int(calendar.timegm(time.gmtime()))
    file_info['file_md5sum'] = get_md5(fpath)
    end_time = int(calendar.timegm(time.gmtime()))
    file_info['md5sum_time'] = end_time - start_time

    return file_info


def is_eof_missing(bam_file):
    process = subprocess.Popen(
            'samtools view ' + bam_file + ' 1:1-1',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    out, err = process.communicate()

    if 'EOF marker is absent' in err:
        return True
    else:
        return False


def compare_file(job):
    # we'd like to do
    # - download bai, check size and md5sum
    # - download bam, check size and md5sum

    for f in ['bai_file', 'bam_file']:
        job_dir = job.job_dir
        object_id = job.job_json.get(f).get('object_id')
        file_name = job.job_json.get(f).get('file_name')
        file_info = download_file_and_get_info(job_dir, object_id, file_name)

        mismatch = False
        if file_info.get('eof_missing'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                f + '-EOF-missing': True
            })
            mismatch = True

        if not file_info.get('file_size') == job.job_json.get(f).get('file_size'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                f + '-size-mismatch': file_info.get('file_size')
            })
            mismatch = True

        if not file_info.get('file_md5sum') == job.job_json.get(f).get('file_md5sum'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                f + '-md5sum-mismatch': file_info.get('file_md5sum')
            })
            mismatch = True

        if file_info.get('download_time'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                f + '-download_time': file_info.get('download_time')
            })

        if file_info.get('md5sum_time'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                f + '-md5sum_time': file_info.get('md5sum_time')
            })

        if mismatch: return False

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
        move_to_next_step(job, 'mismatch')
        return False

    # if everything was fine, finally move the job json file to the next_step folder
    move_to_next_step(job, next_step)
    return True
