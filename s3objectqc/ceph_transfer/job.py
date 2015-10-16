from ..job import Job
from . import downloading, uploadingbyclient


class CEPH_TRANSFER(Job):

    def __init__(self, conf):
        Job.__init__(self, conf, [downloading, uploadingbyclient])
