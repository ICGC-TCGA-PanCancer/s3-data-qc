from ..job import Job
from ..job_tracker import start_a_job
from . import downloading, slicing


class BAM_QC(Job):

    def __init__(self, conf):
    	self.conf = conf
        self.runable = False
        self.tasks = [downloading, slicing]

    	self.job_json_file = start_a_job(self.conf, self.tasks[0].get_name())

        if self.job_json_file:
            self.runable = True


    def run(self):
        if not self.runable:
            # better exception to be added
            return

        print ('\nrunning job: {}'.format(self.job_json_file))

        for task in self.tasks:
            # better logging to be added
            task.run(self.conf, self.job_json_file)
