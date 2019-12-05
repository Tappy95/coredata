import json
import pipeflow
from pipeflow import NsqInputEndpoint, NsqOutputEndpoint
from task_protocol import HYTask
from config import INPUT_NSQ_CONF, OUTPUT_NSQ_CONF, NSQ_NSQD_HTTP_ADDR
from util.pub import pub_to_nsq

WORKER_NUMBER = 1
TOPIC_NAME = 'haiying.amazon.category'


def handle(group, task):
    hy_task = HYTask(task)


async def create_task():
    sites = ['us']
    for site in sites:
        task = {
            "task": "amazon_category_sync"
            "data": {
                "site": "us",
            }
        }
        await pub_to_nsq(NSQ_NSQD_HTTP_ADDR, TOPIC_NAME, json.dumps(task))


def run():
    input_end = NsqInputEndpoint(TOPIC_NAME, 'haiying_crawler', WORKER_NUMBER,  **INPUT_NSQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', WORKER_NUMBER)
    group.set_handle(handle, "thread")
    group.add_input_endpoint('input', input_end)

    group.add_routine_worker(create_task, hour=8, minute=30)
    server.run()
