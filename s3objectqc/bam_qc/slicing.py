import sys
import os
import json
import xmltodict
import time
import subprocess
import calendar
import hashlib
from ..job_tracker import move_to_next_step, get_job_json, save_job_json


name = 'slicing'
next_step = None
s3_bucket_url = 's3://oicr.icgc/data/'

def get_name():
    global name
    return name


def is_diff(job):
    local_bam_file = os.path.join(job.job_dir,
                                job.job_json.get('bam_file').get('file_name')
                            )
    remote_bam_id = job.job_json.get('bam_file').get('object_id')

    samtools_header = get_local_header(local_bam_file)
    dcctool_header = get_remote_header(remote_bam_id)

    is_diff = False
    if header_diff(samtools_header, dcctool_header):
        job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
            'BAM header differs': True
        })
        is_diff = True

    # local slicing
    slice_stats = {}
    for r in job.job_json.get('slice_regions'):
        start_time = int(calendar.timegm(time.gmtime()))
        local_slice_md5sum = local_slicing(local_bam_file, r, job.job_dir)
        end_time = int(calendar.timegm(time.gmtime()))

        slice_stats[r] = {
            'samtools_md5sum': local_slice_md5sum,
            'samtools_time': end_time - start_time
        }

    # remote slicing
    for r in job.job_json.get('slice_regions'):
        start_time = int(calendar.timegm(time.gmtime()))
        remote_slice_md5sum = remote_slicing(remote_bam_id, r, job.job_dir)
        end_time = int(calendar.timegm(time.gmtime()))

        slice_stats.get(r).update({
            'dcctool_md5sum': remote_slice_md5sum,
            'dcctool_time': end_time - start_time
        })

    # comparing slices
    is_diff = is_slices_diff(slice_stats)

    job.job_json.get('_runs_').get(job.conf.get('run_id')).get(get_name()).update({
            'slice_stats': slice_stats
        })


    return is_diff


def is_slices_diff(slice_stats):
    # to be implemented
    # determine whether different or not
    for region in slice_stats:
        if not slice_stats.get(region).get('samtools_md5sum') == \
            slice_stats.get(region).get('dcctool_md5sum'):
            return True

    return False


def local_slicing(bam_file, region, job_dir):
    # to be implemented
    # save slice to local file
    # then get effective md5sum

    out_file = region + '.samtools.sam'
    out_file = out_file.replace(':','-')
    command =   'cd {} && '.format(job_dir) + \
                'samtools view ' + bam_file + \
                ' ' + region + ' > ' + \
                out_file

    process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    out, err = process.communicate()

    if process.returncode:
        # should not exit for just this error, improve it later
        sys.exit('Unable to perform local samtools slice.\nError message: {}'.format(err))

    original_slice_file = os.path.join(job_dir, out_file)
    normalized_slice_file = normalize_sam(original_slice_file)

    return get_md5(normalized_slice_file)


def remote_slicing(bam_id, region, job_dir):
    # to be implemented
    # save slice to local file
    # then get effective md5sum

    out_file = region + '.dcctool.sam'
    out_file = out_file.replace(':','-')
    command =   'cd {} && '.format(job_dir) + \
                'col-repo view --output-type SAM --object-id ' + bam_id + ' ' + \
                '--query ' + region + ' > ' + \
                out_file

    process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    out, err = process.communicate()

    if process.returncode:
        # should not exit for just this error, improve it later
        #print('Unable to perform remote slice.\nError message: {}'.format(err))
        return 'unable_to_slice'

    original_slice_file = os.path.join(job_dir, out_file)
    normalized_slice_file = normalize_sam(original_slice_file)

    return get_md5(normalized_slice_file)


def normalize_sam(original_slice_file):
    normalized_slice_file = original_slice_file + '.normalized'
    norm = open(normalized_slice_file, 'w')

    with open(original_slice_file, 'r') as f:
        for line in f:
            line = line.rstrip('\n')
            fields = line.split('\t')
            fixed_fields = fields[:10]
            attribs = sorted(fields[10:])
            norm.write("\t".join(fixed_fields + attribs) + '\n')

    norm.close()

    return normalized_slice_file


def get_md5(fname):
    hash = hashlib.md5()
    with open(fname) as f:
        for chunk in iter(lambda: f.read(4096), ""):
            hash.update(chunk)
    return hash.hexdigest()


def get_local_header(local_bam_file):
    # to be implemented
    # save header to local file
    # then get effective md5sum
    return ''


def get_remote_header(local_bam_file):
    # to be implemented
    # save header to local file
    # then get effective md5sum
    return ''


def header_diff(samtools_header, dcctool_header):
    # to be implemented
    return False


def _start_task(job):
    job.job_json.get('_runs_').get(job.conf.get('run_id'))[get_name()] = {
        'start': int(calendar.timegm(time.gmtime()))
    }


def run(job):
    global name, next_step
    print ('running task: {}'.format(get_name()))

    _start_task(job)

    if is_diff(job): # file does not match
        move_to_next_step(job, 'mismatch')
    else:
        move_to_next_step(job, 'match')
