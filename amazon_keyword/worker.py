import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from config import INPUT_NSQ_CONF, OUTPUT_NSQ_CONF

WORKER_NUMBER = 2


def handle(group, task):
    hy_task = HYTask(task)


def run():
    input_end = NsqInputEndpoint('haiying.amazon.keyword', 'haiying_crawler', WORKER_NUMBER,  **INPUT_NSQ_CONF)
    output_end = NsqOutputEndpoint(**OUTPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end, ('test',))
    server.run()
