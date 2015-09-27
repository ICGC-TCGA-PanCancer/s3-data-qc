from ..job import Job
from . import downloading


class META_XML_QC(Job):

    def __init__(self, conf):
        Job.__init__(self, conf, [downloading])
