from job_tracker import start_a_job
from bam_qc import downloading, slicing


class Job:
    tasks = [downloading, slicing]

    def __init__(self, conf):
    	self.conf = conf
        self.runable = False
        self.remaining_tasks = Job.tasks

    	self.job_json_file = start_a_job(self.conf, self.remaining_tasks[0].get_name())

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
