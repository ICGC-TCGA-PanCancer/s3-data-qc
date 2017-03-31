from ..job import Job
from . import downloading, uploading


class EGACO_TRANSFER(Job):

    def __init__(self, conf):
        Job.__init__(self, conf, [downloading, uploading])
