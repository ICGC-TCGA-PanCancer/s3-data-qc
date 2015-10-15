from ..job import Job
from . import downloading, uploading_client


class CEPH_TRANSFER(Job):

    def __init__(self, conf):
        Job.__init__(self, conf, [downloading, uploading_client])
