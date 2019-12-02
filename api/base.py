import time
import hashlib
import aiohttp
import requests
from util.log import logger


class BaseAPI:

    u_name = 'bailun'
    skey = '88UN2AZ1kUfi1HAg'
    address = 'http://111.231.88.85:38080'
    api = None
    param = None

    def sign(self):
        timestamp = str(int(time.time()))
        raw_data = self.u_name + self.skey + ''.join(str(v) for v in self.param.values()) + \
                str(int(timestamp))
        md5 = hashlib.md5(raw_data.encode('utf-8'))
        signature = md5.hexdigest()
        self.param.update({"u_name": self.u_name, "time": timestamp, "sign": signature})
        self.param.move_to_end("u_name", last=False)

    def request(self, timeout):
        self.sign()
        try:
            url = self.address + self.api
            resp = requests.post(url=url, data=self.param, timeout=timeout)
            if resp.status_code != 200:
                logger.error("[status code error] code: {} url: {}".format(
                            resp.status_code, url))
            else:
                return json.loads(resp.text)
        except Exception as exc:
            exc_info = (type(exc), exc, exc.__traceback__)
            logger.error("[request error] url: {}".format(url), exc_info=exc_info)
            exc.__traceback__ = None

    async aio_request(self, timeout):
        self.sign()
        try:
            url = self.address + self.api
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request("POST", url, data=self.param) as resp:
                    if resp.status != 200:
                        logger.error("[status code error] code: {} url: {}".format(
                                    resp.status, url))
                    else:
                        return json.loads(await resp.text())
        except Exception as exc:
            exc_info = (type(exc), exc, exc.__traceback__)
            logger.error("[request error] url: {}".format(url), exc_info=exc_info)
            exc.__traceback__ = None
