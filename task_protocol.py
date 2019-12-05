import zlib
import json
from pipeflow import Task


class BaseTask(Task):

    @property
    def from_name(self):
        return self._from

    @from_name.setter
    def from_name(self, from_name):
        self._from = from_name

    @property
    def to_name(self):
        return self._to

    @to_name.setter
    def to_name(self, to_name):
        self._to = to_name


class HYTask(BaseTask):

    def __init__(self, task):
        if isinstance(task, Task):
            super().__init__(data=task.get_raw_data(), from_name=task.get_from(),
                             to_name=task.get_to(), confirm_handle=task.get_confirm_handle())
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
            self._decoded_data = json.loads(self._data)
        return self._decoded_data

    @data.setter
    def data(self, data):
        self._data = json.dumps(data).encode('utf-8')
        self._decoded_data = data

    def spawn(self, data):
        task = HYTask(self)
        task.data = data
        return task
