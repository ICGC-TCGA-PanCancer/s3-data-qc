from ..job import Job
from . import downloading, uploading, slicing


class CEPH_QC(Job):

    def __init__(self, conf):
        Job.__init__(self, conf, [downloading, uploading, slicing])
