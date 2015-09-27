import os
import sys
import yaml
import uuid
import time
import calendar
from random import randint
import subprocess

def _create_output_dir(file, conf, run_dir):
    conf_dir = os.path.dirname(os.path.abspath(file))
    if run_dir:
        run_dir = run_dir.rstrip('/')
        conf['run_id'] = run_dir
        output_dir = os.path.join(conf_dir, conf.get('run_id'))
    else:
        # output dir is under the same dir as the config file
        epoch_time = str(int(calendar.timegm(time.gmtime())))
        uuid_str = str(uuid.uuid4())

        # need to ensure we don't overwrite previous output (if exists)
        # run the loop until the output dir does not exist
        while os.path.isdir(os.path.join(conf_dir, 'run_' + epoch_time + '_' + uuid_str)):
            time.sleep(randint(1,5))
            epoch_time = str(int(calendar.timegm(time.gmtime())))

        conf['run_id'] = 'run_' + epoch_time + '_' + uuid_str
        output_dir = os.path.join(conf_dir, conf.get('run_id'))
        os.makedirs(output_dir)

    return output_dir


def _git_clone(conf):
    git_url = conf.get('job_git_repo')
    run_output_dir = conf.get('run_output_dir')

    process = subprocess.Popen(
            'cd {} && git clone {}'.format(run_output_dir, git_url),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    
    out, err = process.communicate()
    if process.returncode:
        sys.exit('Unable to clone GitHub repo for jobs, please ensure it\'s accessible.\nError message: {}'.format(err))


def get_config(file, run_dir):

    conf = {}
    with open(file) as f:
        conf = yaml.safe_load(f)

    # stop if conf does not include job_git_repo
    if not conf.get('job_git_repo'): sys.exit('Error: job_git_repo must be set in the configuration file!')
    if not conf.get('job_type'): sys.exit('Error: job_type must be set in the configuration file!')

    output_dir = _create_output_dir(file, conf, run_dir)

    git_local_dir = os.path.join(output_dir, \
                                   conf.get('job_git_repo').split('/')[-1].replace('.git', '')
                                )

    conf.update({
            'run_output_dir': output_dir,
            'git_local_dir': git_local_dir,
            'job_queue_dir': os.path.join(git_local_dir, conf.get('job_queue_dir'))
        })

    if not run_dir:
        _git_clone(conf)

    return conf
