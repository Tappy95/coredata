from aiohttp import ClientSession, ClientTimeout
from urllib.parse import ParseResult, urlunparse, urlencode
from .log import logger


async def pub_to_nsq(address, topic, msg, timeout=60):
    url = urlunparse(ParseResult(scheme='http', netloc=address, path='/pub', params='',
                                 query=urlencode({'topic': topic}), fragment=''))
    async with ClientSession(timeout=ClientTimeout(total=timeout)) as session:
        async with session.request("POST", url, data=msg) as resp:
            if resp.status != 200:
                logger.error("[pub to nsq error] topic: {}".format(topic))


async def mpub_to_nsq(address, topic, msgs, timeout=60):
    if any(map(lambda x: '\n' in x, msgs)):
        raise ValueError(r"msgs contain \n")
    url = urlunparse(ParseResult(scheme='http', netloc=address, path='/mpub', params='',
                                 query=urlencode({'topic': topic}), fragment=''))
    async with ClientSession(timeout=ClientTimeout(total=timeout)) as session:
        async with session.request("POST", url, data="\n".join(msgs)) as resp:
            if resp.status != 200:
                logger.error("[pub to nsq error] topic: {}".format(topic))
