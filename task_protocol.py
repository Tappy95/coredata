import zlib
import json
from pipeflow import Task


class BaseTask(Task):

    @property
    def from(self):
        return self._from

    @from.setter
    def from(self, from):
        self._from = from

    @property
    def to(self):
        return self._to

    @to.setter
    def to(self, to):
        self._to = to


class HYTask(BaseTask):

    def __init__(self, task):
        if isinstance(task, Task):
            super().__init__(data=task.get_raw_data(), from=task.get_from(),
                             to=task.get_to(), confirm_handle=task.get_confirm_handle())
            self._decoded_data = None
        else:
            raise ValueError('is not a task')

    @property
    def task_type(self):
        return self.data.get('task')

    @property
    def task_data(self):
        return self.data.get('data')

    @property
    def data(self):
        if self._decoded_data is None:
            self._decoded_data = json.loads(zlib.decompress(self._data))
        return self._decoded_data

    @data.setter
    def data(self, data):
        self._data = zlib.compress(json.dumps(data).encode('utf-8'))
        self._decoded_data = data

    def spawn(self, data):
        task = HYTask(self)
        task.data = data
        return task
