import asyncio
import base64
import datetime
import hashlib
import hmac
import json
from copy import deepcopy

import beeprint
from loguru import logger

import websockets
from NoLossAsyncGenerator import NoLossAsyncGenerator

import urllib

from HuobiAsyncWebsocket.UrlParamsBuilder import create_signature, UrlParamsBuilder


# todo 测试重复订阅是否幂等
class HuobiAsyncWs:
    ws_baseurl = 'wss://api-aws.huobi.pro/ws/v2'

    def __init__(self, apikey, secret):
        self._apikey = apikey
        self._secret = secret
        # self._session: aiohttp.ClientSession = None
        self._ws: websockets.WebSocketClientProtocol = None
        self._ws_ok: asyncio.Future = None
        self._handlers = set()
        self._exiting = False

    async def exit(self):
        self._exiting = True
        ws_close_task = None
        if self._ws:
            ws_close_task = asyncio.create_task(self._ws.close())
        if ws_close_task:
            await ws_close_task

    # def _generate_signature_time(self):
    #     request_str = 'GET\n'
    #     request_str += urllib.parse.urlparse(type(self).ws_baseurl)[1] + '\n'
    #     request_str += '/ws/v2\n'
    #     request_str += f"accessKey={self._apikey}"
    #     request_str += '&' + 'signatureMethod=HmacSHA256'
    #     request_str += '&' + 'signatureVersion=2.1'
    #     time_s = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    #     request_str += '&' + f'timestamp={time_s}'
    #
    #     digest = hmac.new(self._secret.encode('utf-8'),
    #                       request_str.encode('utf-8'),
    #                       digestmod=hashlib.sha256).digest()
    #
    #     signature = base64.b64encode(digest)
    #     signature = signature.decode()
    #     return signature, time_s

    async def _pong(self):
        # 心跳检测
        # {
        #     'action': 'ping',
        #     'data': {
        #         'ts': 1597729470150,
        #     },
        # }
        async for ping in self.filter_stream([{'action': 'ping'}]):
            pong = json.dumps({
                "action": "pong",
                "data": {
                    "ts": ping['data']['ts']
                }
            })
            # print(f'准备pong:\n{repr(pong)}')
            await self._ws.send(pong)
            # print('pong发完')
            logger.debug('\n' + beeprint.pp({
                "action": "pong",
                "data": {
                    "ts": ping['data']['ts']
                }
            }, output=False, string_break_enable=False, sort_keys=False))

    async def _get_authentication(self):

        builder = UrlParamsBuilder()
        create_signature(api_key=self._apikey,
                         secret_key=self._secret,
                         method='GET',
                         url=type(self).ws_baseurl,
                         builder=builder)
        auth_request = {
            "action": "req",
            "ch": "auth",
            "params": {
                "authType": "api",
                "accessKey": self._apikey,
                "signatureMethod": "HmacSHA256",
                "signatureVersion": "2.1",
                "timestamp": builder.param_map['timestamp'],
                "signature": builder.param_map['signature']
            }
        }
        await self._ws.send(json.dumps(auth_request))

    async def _ws_manager(self):
        self._ws = await websockets.connect(self.ws_baseurl)
        # 鉴权
        asyncio.create_task(self._get_authentication())

        if not self._ws_ok.done():
            self._ws_ok.set_result(None)
        asyncio.create_task(self._pong())
        async for msg in NoLossAsyncGenerator(self._ws):
            msg = json.loads(msg)
            logger.debug('\n' + beeprint.pp(msg, output=False, string_break_enable=False, sort_keys=False))
            tasks = []
            for handler in self._handlers:
                if asyncio.iscoroutinefunction(handler):
                    tasks.append(asyncio.create_task(handler(deepcopy(msg))))
                else:
                    handler(deepcopy(msg))
            [await task for task in tasks]

    @classmethod
    async def create_instance(cls, apikey, secret):
        self = cls(apikey, secret)
        self._ws_ok = asyncio.get_running_loop().create_future()
        # 启动ws管理器
        asyncio.create_task(self._ws_manager())
        await self._ws_ok
        return self

    def filter_stream(self, _filters: list = None):
        '''
        Filter the ws data stream and push the filtered data to the async generator which is returned by the method.
        Remember to explicitly call the close method of the async generator to close the stream.

        stream=huobiasyncws.filter_stream()

        #handle message in one coroutine:
        async for news in stream:
            ...
        #close the stream in another:
        close_task=asyncio.create_task(stream.close())
        ...
        await close_task


        :param _filters:A list of dictionaries, key and value of any of which could all be matched by some message, then the message would be filtered.
        :return:
        '''
        if _filters is None:
            _filters = []

        ag = NoLossAsyncGenerator(None)

        def handler(msg):
            if (_filters and any(
                    [all([((key in msg) and (value == msg[key])) for key, value in _filter.items()]) for _filter in
                     _filters])) \
                    or not _filters:
                ag.q.put_nowait(msg)

        self._handlers.add(handler)
        _close = ag.close

        async def close():
            self._handlers.remove(handler)
            await _close()

        ag.close = close
        return ag

    def order_stream(self):
        '''
        Filter the ws order data stream and push the filtered data to the async generator which is returned by the method.
        Remember to explicitly call the close method of the async generator to close the stream.


        stream=binancews.order_stream()

        #handle message in one coroutine:
        async for news in stream:
            ...
        #close the stream in another:
        close_task=asyncio.create_task(stream.close())
        ...
        await close_task

        :return:
        '''
        return self.filter_stream([{"e": "executionReport"}])


if __name__ == '__main__':
    pass
