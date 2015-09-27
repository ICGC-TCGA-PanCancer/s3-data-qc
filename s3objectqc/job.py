import os
import re
from job_tracker import start_a_job, get_job_json


class Job():

    def __init__(self, conf, tasks):
    	self.conf = conf
        self.runable = False
        self.tasks = tasks

        # starting a job run with its first task
        self.job_json_file = start_a_job(self)

        if self.job_json_file:
            self.job_json = get_job_json(self)

            # let's use json file name as job id for now
            self.job_id = re.sub(r'\.json$', '', self.job_json_file)
            # set up working directory for the job
            worker_output_dir = self.conf.get('worker_output_dir')
            self.job_dir = os.path.join(worker_output_dir, self.job_id)
            os.mkdir(self.job_dir)

            self.runable = True


    def _record_run_info(self):
        if not self.job_json.get('_runs_'):
            self.job_json['_runs_'] = {
                self.conf.get('worker_id'): {}
            }
        else:
            self.job_json.get('_runs_').update({
                self.conf.get('worker_id'): {}
            })


    def run(self):
        if not self.runable:
            # better exception to be added
            return

        print ('\nrunning job: {}'.format(self.job_json_file))
        self._record_run_info()

        while len(self.tasks) > 0:
            # better logging to be added
            if self.tasks[0].run(self): # previous task ran well
                self.tasks.pop(0)
            else:  # previous task indicates job needs be terminated earlier
                break

