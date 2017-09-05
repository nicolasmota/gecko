

class STATUS:

    ASSIGNED = 'ASSIGNED'
    CANCELED = 'CANCELED'
    DONE = 'DONE'
    ERROR = 'ERROR'
    NEW = 'NEW'
    PREPARED = 'PREPARED'
    SCHEDULED = 'SCHEDULED'
    STARTED = 'STARTED'
    UNRIPE = 'UNRIPE'


class CeleryTask:

    def __init__(self, *args, **kwargs):
        self.task = None
        self.status = None
        self.process = None
        self.task_id = None

        super(CeleryTask, self).__init__(*args, **kwargs)

    def set_status(self, value):
        """ Set status on celery task """
        if self.task:
            self.status = value

    def get_status(self):
        if self.task:
            return self.status
        return STATUS.UNRIPE

    def initialize(self, task):
        self.task = task

    def run(self, *args):
        self.task.delay(*args)
