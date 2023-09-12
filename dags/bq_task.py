from abstract_task import AbstractTask
class BigQueryTask(AbstractTask):
    def __init__(self, *args, **kwargs):
        self.type = 'bigquery'
        self.args = args
        self.kwargs = kwargs
        
    def  create(self):
        return BigQueryTask(*self.args, **self.kwargs)
