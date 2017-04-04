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
from icgconnect import collab, utils, xml_audit



name = 'uploading'
next_step = None
data_bucket_url = 's3://oicr.icgc/data/'
meta_bucket_url = 's3://oicr.icgc.meta/metadata/'

def get_name():
    global name
    return name

def _start_task(job):
    job.job_json.get('_runs_').get(job.conf.get('run_id'))[get_name()] = {
        'start': int(calendar.timegm(time.gmtime()))
    }

def _upload(job, collab_upload_token):
    job_json = job.job_json
    input_file = job_json.get('files')[0].get('file_name')
    print input_file

    if not os.path.isfile(input_file):
        return

    xml_folder = job.conf.get('xml_folder_path')
    bai_file_name = input_file+".bai"
    xml_file_name = input_file+".xml"
    manifest_file = "manifest_"+job_json.get('bundle_id')+".txt"

    try:

        if len(job_json.get('files'))==1:
            job_json.get('files').append({})

        utils.file_utils.generate_bai_from_bam(input_file, bai_file_name)

        job_json.get('files')[1]['type'] = "bai"
        job_json.get('files')[1]['file_name'] = bai_file_name
        job_json.get('files')[1]['file_md5sum'] = utils.file_utils.get_file_md5(bai_file_name)
        job_json.get('files')[1]['file_size'] = os.path.getsize(bai_file_name)
        job_json.get('files')[1]['object_id'] = collab.filename_get_post(job_json.get('bundle_id'),collab_upload_token,bai_file_name,job_json.get('project_code'))['id']

        xml_audit.quick_generate(xml_folder+job_json.get('project_code'), xml_file_name,
            job_json.get('ega_dataset'),
            job_json.get('ega_sample'),
            job_json.get('ega_study'),
            job_json.get('ega_run'),
            job_json.get('bundle_id'))

        if len(job_json.get('files'))==2:
            job_json.get('files').append({})

        job_json.get('files')[2]['type'] = "xml"
        job_json.get('files')[2]['file_name'] = xml_file_name
        job_json.get('files')[2]['file_md5sum'] = utils.file_utils.get_file_md5(xml_file_name)
        job_json.get('files')[2]['file_size'] = os.path.getsize(xml_file_name)
        job_json.get('files')[2]['object_id'] = collab.filename_get_post(job_json.get('bundle_id'),collab_upload_token,xml_file_name,job_json.get('project_code'))['id']

        collab.generate_manifest_file(manifest_file, job_json.get('files'))
        collab.validate_manifest_file(job_json.get('bundle_id'), manifest_file)
        collab.upload(manifest_file,job.conf.get('icgc_storage_client'))
        collab.delete_manifest_file(manifest_file, True)
	move_to_next_step(job, 'completed')

    except ValueError, err:
        move_to_next_step(job, 'failed')
        print str(err)
        return
    except KeyError, err:
        move_to_next_step(job, 'failed')
        print str(err)
        return

def run(job):
    global name, next_step
    print ('running task: {}'.format(get_name()))

    _start_task(job)

    _upload(job, job.conf.get('collab_upload_token'))
    #move_to_next_step(job, 'completed')
    return True
