import os
import re
from ..job import Job
from ..job_tracker import start_a_job, get_job_json
from . import downloading


class META_XML_QC(Job):

    def __init__(self, conf):
    	self.conf = conf
        self.runable = False
        self.tasks = [downloading]

    	self.job_json_file = start_a_job(self.conf, self.tasks[0].get_name())

        if self.job_json_file:
            self.job_json = get_job_json(self.conf,
                                self.tasks[0].get_name(),
                                self.job_json_file)

            # let's use json file name as job id for now
            self.job_id = re.sub(r'\.json$', '', self.job_json_file)
            # set up working directory for the job
            worker_output_dir = self.conf.get('worker_output_dir')
            self.job_dir = os.path.join(worker_output_dir, self.job_id)
            os.mkdir(self.job_dir)

            self.runable = True


    def run(self):
        if not self.runable:
            # better exception to be added
            return

        print ('\nrunning job: {}'.format(self.job_json_file))
        if not self.job_json.get('_runs_'):
            self.job_json['_runs_'] = {
                self.conf.get('worker_id'): {}
            }
        else:
            self.job_json.get('_runs_').update({
                self.conf.get('worker_id'): {}
            })

        for task in self.tasks:
            # better logging to be added
            ret = task.run(self.conf, self)
            if not ret: # step terminated before reaching the end
                break

