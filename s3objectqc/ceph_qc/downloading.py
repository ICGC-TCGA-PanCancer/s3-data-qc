import sys
import os
import re
import json
import xmltodict
import time
import subprocess
import calendar
from ..job_tracker import move_to_next_step, get_job_json
from ..util import get_md5
import shutil


name = 'downloading'
next_step = 'uploading'
bucket_url = 's3://oicr.icgc/data/'


def get_name():
    global name
    return name


def download_file_and_get_info(job_dir, object_id, file_name, gnos_id):
    global bucket_url

    file_info = {
        'file_size': None,
        'file_md5sum': None
    }

    fpath = os.path.join(job_dir, gnos_id, file_name)
    # if not os.path.isdir(job_dir+'/'+gnos_id): os.mkdir(job_dir+'/'+gnos_id)

    start_time = int(calendar.timegm(time.gmtime()))

    # Only download when file does not already exist.
    # - This is meant more for repeative testing/debugging without
    #   having to download large file over and over again.
    # - In real world, shouldn't have as each time a new run dir is created
    # if not os.path.isfile(fpath):

    command =   'cd {} && '.format(job_dir) + \
                'icgc-storage-client --profile aws download --object-id ' + object_id + ' --output-dir . --index false --output-layout bundle'

    process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    out, err = process.communicate()

    if process.returncode:
        # should not exit for just this error, improve it later
        file_info['error'] = 'icgc client tools download failed'
        return file_info

    end_time = int(calendar.timegm(time.gmtime()))

    file_info['download_time'] = end_time - start_time

    start_time = int(calendar.timegm(time.gmtime()))

    if file_name.endswith('.xml'):
        with open(fpath, 'r') as f: xml_str = f.read()
        data = re.sub(r'<ResultSet .+?>', '<ResultSet>', xml_str)
        with open(fpath + '.temp', 'w') as f:
            f.write(data)
        file_info['file_size'] = os.path.getsize(fpath + '.temp')
        file_info['file_md5sum'] = get_md5(fpath + '.temp', True)
        os.remove(fpath + '.temp')
    else:
        file_info['file_size'] = os.path.getsize(fpath)
        # run a quick check here to see how EOF is missing
        # comment out for debug
        if file_name.endswith('.bam') and is_eof_missing(fpath):
            file_info['eof_missing'] = True
            return file_info
        file_info['file_md5sum'] = get_md5(fpath, True)
    
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

def compare_vcf_files(job):
    gnos_id = job.job_json.get('gnos_id')
    job_dir = job.job_dir
    for f in job.job_json.get('files'):
        object_id = f.get('object_id')
        file_name = f.get('file_name')
        file_info = download_file_and_get_info(job_dir, object_id, file_name, gnos_id)
        
        if file_info.get('error'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                'icgc_client_tool-download-error': True
            })        
            return False

        mismatch = False
        if not file_info.get('file_size') == f.get('file_size'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                file_name + '-size-mismatch': file_info.get('file_size')
            })
            mismatch = True

        # only need this comparison when file_md5sum was computed
        if file_info.get('file_md5sum') is not None and \
           not file_info.get('file_md5sum') == f.get('file_md5sum'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                file_name + '-md5sum-mismatch': file_info.get('file_md5sum')
            })
            mismatch = True

        if file_info.get('download_time') is not None:
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                file_name + '-download_time': file_info.get('download_time')
            })

        if file_info.get('md5sum_time') is not None:
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                file_name + '-md5sum_time': file_info.get('md5sum_time')
            })

        if mismatch: return False

    return True


def compare_file(job):
    # we'd like to do
    # - download bai, check size and md5sum
    # - download bam, check size and md5sum
    gnos_id = job.job_json.get('gnos_id')
    job_dir = job.job_dir

    # compare xml file
    f = 'xml_file'
    object_id = job.job_json.get(f).get('object_id')
    file_name = job.job_json.get(f).get('file_name')
    file_info = download_file_and_get_info(job_dir, object_id, file_name, gnos_id)
    if file_info.get('error'):
        job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
            'icgc_client_tool-download-error': True
        })        
        return False

    mismatch = True

    for repo in job.job_json.get('available_repos'):
        v = repo.values()[0]
        if file_info.get('file_md5sum') is not None and file_info.get('file_size') is not None and \
        file_info.get('file_md5sum') == v.get('file_md5sum') and file_info.get('file_size') == v.get('file_size'):
            mismatch = False
            break
    
    if mismatch: return False


    for f in ['bai_file', 'bam_file']:
        #job_dir = job.job_dir
        object_id = job.job_json.get(f).get('object_id')
        file_name = job.job_json.get(f).get('file_name')

        file_info = download_file_and_get_info(job_dir, object_id, file_name, gnos_id)
        if file_info.get('error'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                'icgc_client_tool-download-error': True
            })        
            return False

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

        # only need this comparison when file_md5sum was computed
        if file_info.get('file_md5sum') is not None and \
           not file_info.get('file_md5sum') == job.job_json.get(f).get('file_md5sum'):
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                f + '-md5sum-mismatch': file_info.get('file_md5sum')
            })
            mismatch = True

        if file_info.get('download_time') is not None:
            job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
                f + '-download_time': file_info.get('download_time')
            })

        if file_info.get('md5sum_time') is not None:
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

    if job.job_json.get('data_type').endswith('-VCF'):
        compare = compare_vcf_files(job)
        if compare:
            local_file_dir = os.path.join(job.job_dir, job.job_json.get('gnos_id'))
            # remove the HUGH bam file when match
            if os.path.exists(local_file_dir): shutil.rmtree(local_file_dir, ignore_errors=True)          
            move_to_next_step(job, "match")
            return False 
    elif job.job_json.get('data_type').startswith('WGS-BWA'):
        compare = compare_file(job)
        if compare:
            # if everything was fine, finally move the job json file to the next_step folder
            move_to_next_step(job, next_step)
            return True
    else:
        sys.exit('Unknown data type.\nError message: {}'.format(job.job_json.get('data_type')))

    if not compare: # file does not match
        move_to_next_step(job, 'mismatch')
        return False

