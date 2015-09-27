from ..job import Job
from . import downloading, slicing


class BAM_QC(Job):

    def __init__(self, conf):
        Job.__init__(self, conf, [downloading, slicing])
