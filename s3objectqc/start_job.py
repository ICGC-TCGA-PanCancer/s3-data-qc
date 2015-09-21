import subprocess
import os
import sys
from random import randint
import time

def start_next_job(conf):
    git_local_dir = conf.get('git_local_dir')
    job_queue_dir = conf.get('job_queue_dir')

    job_file = ''

    for i in range(5): # try at most 5 times
        # step 1: syn up with github
        command = 'cd {} && '.format(git_local_dir) + \
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
        command = 'cd {} && '.format(os.path.join(git_local_dir, job_queue_dir, 'queued-jobs')) + \
                  'find . -name "*.json" |head -1|awk -F"/" \'{print $2}\' '

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
            time.sleep(randint(1,10))  # pause a few seconds before retry
            continue  # try again

        # step 3: git move the job file from queued-jobs to downloading-jobs folder, then commit and push
        command = 'cd {} && '.format(os.path.join(git_local_dir, job_queue_dir)) + \
                  'git mv {} {} && '.format(os.path.join(git_local_dir, job_queue_dir, 'queued-jobs', job_file),
                                            os.path.join(git_local_dir, job_queue_dir, 'downloading-jobs', job_file)) + \
                  'git commit -m \'downloading: {}\' && '.format(job_file) + \
                  'git push'

        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        out, err = process.communicate()

        if process.returncode == 0:
            break  # succeeded
        else:
            print('Unable to fetch new job.\nError message: {}\n\nRetrying...'.format(err))
            time.sleep(randint(1,10))  # pause a few seconds before retry

    return job_file

