import aiohttp
from urllib.parse import ParseResult, urlunparse, urlencode
from .log import logger


async def pub_to_nsq(address, topic, msg, timeout=20):
    url = urlunparse(ParseResult(scheme='http', netloc='address', path='/pub', params='',
                                 query=urlencode({'topic': topic}), fragment=''))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.request("POST", url, data=msg) as resp:
            if resp.status != 200:
                logger.error("[pub to nsq error] topic: {}".format(topic))