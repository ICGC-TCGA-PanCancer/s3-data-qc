import sys
import os
import json
import xmltodict
import time
import subprocess
import calendar
from ..job_tracker import move_to_next_step, get_job_json, save_job_json


name = 'downloading'
next_step = None
s3_bucket_url = 's3://oicr.icgc/data/'

def get_name():
    global name
    return name


def compare_file(job):
    # get file size
    file_info = get_file_info(job.job_json)

    s3_file_info = download_xml_get_s3_file_info(
            job.job_dir,
            file_info.get('xml_file').get('object_id'),
            file_info.get('xml_file').get('file_name')
        )
    if s3_file_info.get('s3_bam_size') == file_info.get('bam_file').get('file_size') and \
        s3_file_info.get('s3_bam_md5') == file_info.get('bam_file').get('file_md5sum') and \
        s3_file_info.get('s3_bai_size') == file_info.get('bai_file').get('file_size') and \
        s3_file_info.get('s3_bai_md5') == file_info.get('bai_file').get('file_md5sum'):

        return True
    else:
        job.job_json.get('_runs_').get(job.conf.get('worker_id')).get(name).update({
                's3_bam_size': s3_file_info.get('s3_bam_size'),
                's3_bam_md5': s3_file_info.get('s3_bam_md5'),
                's3_bai_size': s3_file_info.get('s3_bai_size'),
                's3_bai_md5': s3_file_info.get('s3_bai_md5')
            })

        generate_correct_json_file(job, s3_file_info)

        return False


def generate_correct_json_file(job, s3_file_info):
    job_json_file_name = job.job_json_file
    global name
    new_json_obj = get_job_json(job)

    # remove possible _runs_ data
    if new_json_obj.get('_runs_'): del new_json_obj['_runs_']

    for f in new_json_obj.get('files'):
        if f.get('file_name').endswith('.bam'):
            f['file_size'] = s3_file_info.get('s3_bam_size')
            f['file_md5sum'] = s3_file_info.get('s3_bam_md5')
        if f.get('file_name').endswith('.bai'):
            f['file_size'] = s3_file_info.get('s3_bai_size')
            f['file_md5sum'] = s3_file_info.get('s3_bai_md5')

    with open(os.path.join(job.job_dir, job_json_file_name), 'w') as f:
        f.write(json.dumps(new_json_obj, indent=4, sort_keys=True))


def get_file_info(job_json):
    file_info = {
        'xml_file': {},
        'bam_file': {},
        'bai_file': {}
    }

    for f in job_json.get('files'):
        file_type = None
        if f.get('file_name').endswith('.xml'):
            file_type = 'xml_file'
        elif f.get('file_name').endswith('.bam'):
            file_type = 'bam_file'
        elif f.get('file_name').endswith('.bai'):
            file_type = 'bai_file'

        file_info.get(file_type).update(f)

    return file_info


def download_xml_get_s3_file_info(job_dir, object_id, file_name):
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
        file_info = {}
        with open (os.path.join(job_dir, file_name), 'r') as x: xml_str = x.read()
        files = xmltodict.parse(xml_str).get('ResultSet').get('Result').get('files').get('file')
        for f in files:
            if f.get('filename').endswith('.bam'):
                file_info['s3_bam_size'] = int(f.get('filesize'))
                file_info['s3_bam_md5'] = f.get('checksum').get('#text')
            if f.get('filename').endswith('.bai'):
                file_info['s3_bai_size'] = int(f.get('filesize'))
                file_info['s3_bai_md5'] = f.get('checksum').get('#text')

        return file_info


def run(job):
    global name, next_step
    print ('running task: {}'.format(get_name()))

    # many steps here
    job.job_json.get('_runs_').get(job.conf.get('worker_id'))[name] = {
        'start': int(calendar.timegm(time.gmtime()))
    }

    ret = compare_file(job)

    if not ret: # file does not match
        job.job_json.get('_runs_').get(job.conf.get('worker_id')).get(name).update({
                'stop': int(calendar.timegm(time.gmtime()))
            })

        save_job_json(job)

        # set the tasks list to empty as the job will now end
        move_to_next_step(job, 'mismatch')
        job.tasks = []

        return False

    job.job_json.get('_runs_').get(job.conf.get('worker_id')).get(name).update({
            'stop': int(calendar.timegm(time.gmtime()))
        })

    save_job_json(job)

    # if everything was fine, finally move the job json file to the next_step folder
    move_to_next_step(job, 'match')
    job.tasks = []
