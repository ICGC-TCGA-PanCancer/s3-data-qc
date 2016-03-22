import subprocess
import os
import sys
import glob
import json
from random import random
import time
import calendar

def start_a_job(job):
    job_queue_dir = job.conf.get('job_queue_dir')
    next_step_name = job.tasks[0].get_name() # first task of the job
    max_times = job.conf.get('max_times')

    job_file = ''

    for i in range(max_times): # try until succeed
        # step 1: syn up with github
        command = 'cd {} && '.format(job_queue_dir) + \
                  'git checkout master && ' + \
                  'git reset --hard origin/master && ' + \
                  'git pull'
        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        out, err = process.communicate()

        # step 2: find next job json file
        # first look into retry-jobs directory
        job_source_dir = 'queued'
        job_file = _get_retry_job(job_queue_dir, job.conf.get('run_id'))
        used_percent = _get_used_percent(job)
        percent_limit_1 = float(job.conf.get('instance_space_limit_1'))
        percent_limit_2 = float(job.conf.get('instance_space_limit_2'))

        if not used_percent:
            print('WARNing, unable to get the used space percentage for the instance!')
            time.sleep(random())  # pause a few seconds before retry
            continue  # try again
        elif used_percent>percent_limit_2:
            print('WARNing, Please release the space of the instance!')
            time.sleep(random())  # pause a few seconds before retry
            continue  # try again
        elif used_percent>percent_limit_1 and used_percent<=percent_limit_2:
            job_source_dir = 'retry'
        elif job_file and used_percent<=percent_limit_1:
            job_source_dir = 'retry'
        else:
            command = 'cd {} && '.format(os.path.join(job_queue_dir, 'queued-jobs')) + \
                      'find . -name "*.json" | sort |head -1 | awk -F"/" \'{print $2}\' '

            process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            job_file, err = process.communicate()
            job_file = job_file.rstrip()

        #print('job: {}'.format(job_file))  # for debugging
        if not job_file:
            time.sleep(random())  # pause a few seconds before retry
            continue  # try again

        # step 3: git move the job file from queued-jobs to downloading-jobs folder, then commit and push
        command = 'cd {} && '.format(os.path.join(job_queue_dir)) + \
                  'git checkout master && ' + \
                  'git reset --hard origin/master && ' + \
                  'git pull &&' + \
                  'git mv {} {} && '.format(os.path.join(job_queue_dir, job_source_dir + '-jobs', job_file),
                                            os.path.join(job_queue_dir, next_step_name + '-jobs', job_file)) + \
                  'git commit -m \'{} to {}: {} in {}\' && '.format(job_source_dir,
                            next_step_name, job_file, job.conf.get('run_id')) + \
                  'git push'

        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        out, err = process.communicate()

        if not process.returncode:
            break  # succeeded
        else:
            print('Unable to fetch new job.\nError message: {}\n\nRetrying...'.format(err))
            time.sleep(random())  # pause a few seconds before retry

    return job_file

def _get_used_percent(job):
    used_percent = None
    command = 'df -h |grep /dev/vda1'
    process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    out, err = process.communicate()
    if not process.returncode:
        used_percent_str = out.rstrip(' /\n').split()[-1]
        used_percent = float(used_percent_str.strip('%'))/100

    else:
        print('Unable to get the free space of the instance.\nError message: {}\n\nRetrying...'.format(err))
    
    return used_percent    


def _get_retry_job(job_queue_dir, run_id):
    retry_job_files = os.path.join(job_queue_dir, 'retry-jobs', '*.json')
    for job_file in glob.glob(retry_job_files):
        with open(job_file) as f:
            job_json = json.load(f)
            if job_json.get('_runs_', {}).get(run_id):
                return os.path.basename(job_file)


# this function retrieves the job_json_file and parse it and return
def get_job_json(job):
    conf = job.conf
    current_step_name = job.tasks[0].get_name()
    job_json_file_name = job.job_json_file

    json_file = os.path.join(conf.get('job_queue_dir'),
                                current_step_name + '-jobs',
                                job_json_file_name)

    with open(json_file) as data_file:
        json_obj = json.load(data_file)
        return json_obj


# this function serialize the job_json (after some updates) to job_json_file
def save_job_json(job):
    conf = job.conf
    current_step_name = job.tasks[0].get_name()
    job_json_file_name = job.job_json_file
    job_json = job.job_json
    max_times = job.conf.get('max_times')

    json_file = os.path.join(conf.get('job_queue_dir'),
                                current_step_name + '-jobs',
                                job_json_file_name)

    with open(json_file, 'w') as f:
        f.write(json.dumps(job_json, indent=4, sort_keys=True))

    for i in range(max_times): # try untill succeed
        command = 'cd {}'.format(conf.get('job_queue_dir'))

        if i == 0:
            command = command + ' && ' + 'git add {} && '.format(json_file) + \
                        'git commit -m \'save info at {}: {} in {}\''.format(current_step_name,
                                                                job_json_file_name, job.conf.get('run_id'))
        command = command + ' && ' + 'git pull --no-edit && git push'

        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        out, err = process.communicate()
        if not process.returncode:
            break  # succeeded
        else:
            print('Unable to save the job json file.\nError message: {}\n\nRetrying...'.format(err))
            time.sleep(random())  # pause a few seconds before retry


def move_to_next_step(job, next_step_name):
    conf = job.conf
    current_step_name = job.tasks[0].get_name()
    job_json_file_name = job.job_json_file
    max_times = job.conf.get('max_times')

    start_time = job.job_json.get('_runs_').get(job.conf.get('run_id')).get(current_step_name).get('start')
    end_time = int(calendar.timegm(time.gmtime()))
    time_spent = end_time - start_time
    job.job_json.get('_runs_').get(job.conf.get('run_id')).get(current_step_name).update({
            'stop': end_time,
            'time': time_spent
        })

    save_job_json(job)

    job_queue_dir = conf.get('job_queue_dir')

    print ('move from {} to {}'.format(current_step_name, next_step_name))

    for i in range(max_times): # try untill succeed

        command = 'cd {} && '.format(job_queue_dir) + \
                  'git checkout master && ' + \
                  'git reset --hard origin/master && ' + \
                  'git pull && ' + \
                  'git mv {} {} && '.format(os.path.join(job_queue_dir, current_step_name + '-jobs', job_json_file_name),
                                            os.path.join(job_queue_dir, next_step_name + '-jobs', job_json_file_name)) + \
                  'git commit -m \'{} to {}: {} in {}\' && '.format(current_step_name,
                        next_step_name, job_json_file_name, job.conf.get('run_id')) + \
                  'git push'

        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        out, err = process.communicate()

        if not process.returncode:
            break  # succeeded
        else:
            print('Unable to move the job json file.\nError message: {}\n\nRetrying...'.format(err))
            time.sleep(random())  # pause a few seconds before retry
