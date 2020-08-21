import asyncio
import json
import traceback
from copy import deepcopy

import beeprint
from loguru import logger

import websockets
from NoLossAsyncGenerator import NoLossAsyncGenerator
from ensureTaskCanceled import ensureTaskCanceled

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
        self._update_ws_task: asyncio.Task = None
        self._ws_generator: NoLossAsyncGenerator = None
        self._update_ws_event = asyncio.Event()  # 用来提示所有的需要更新连接的情况
        self._update_ws_event.set()  # 刚开始就需要更新连接，最初新建

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
        ping_aiter = self.stream_filter([{'action': 'ping'}])
        while True:
            try:
                # 等心跳只能等30s，否则超时
                ping = await asyncio.wait_for(ping_aiter.__anext__(), 30)
            except asyncio.TimeoutError:  # 等心跳超时 todo 测试故意心跳超时
                self._update_ws_event.set()
            else:
                pong = json.dumps({
                    "action": "pong",
                    "data": {
                        "ts": ping['data']['ts']
                    }
                })
                await self._ws.send(pong)
                logger.debug('\n' + beeprint.pp({
                    "action": "pong",
                    "data": {
                        "ts": ping['data']['ts']
                    }
                }, output=False, string_break_enable=False, sort_keys=False))

    async def _get_authentication(self):
        '''不鉴权无心跳'''

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

    async def _update_ws(self, old_ws):
        '''
        安全处理老的ws连接，建立新的ws连接

        :param old_ws: 老的ws连接
        :return:
        '''
        # 安全退出老的ws
        if isinstance(self._ws_generator, NoLossAsyncGenerator):
            await self._ws_generator.close()
        if isinstance(old_ws, asyncio.Task) and not old_ws.done():
            asyncio.create_task(ensureTaskCanceled(old_ws))

        self._ws = await websockets.connect(self.ws_baseurl)
        logger.info('New huobi ws connection opened.')
        # 鉴权
        asyncio.create_task(self._get_authentication())

        # 如果被删除，负责关闭自己打开的ws连接
        def cancel_ws(task: asyncio.Task):
            async def close_old_ws(ws):
                await asyncio.create_task(ws.close())
                logger.info('Old huobi ws connection closed.')

            asyncio.create_task(close_old_ws(task._opened_ws))

        self._update_ws_task._opened_ws = self._ws
        self._update_ws_task.add_done_callback(cancel_ws)
        # 通知实例化完成
        if not self._ws_ok.done():
            self._ws_ok.set_result(None)
        self._ws_generator = NoLossAsyncGenerator(self._ws)
        async for msg in self._ws_generator:
            try:
                msg = json.loads(msg)
                logger.debug('\n' + beeprint.pp(msg, output=False, string_break_enable=False, sort_keys=False))
                tasks = []
                for handler in self._handlers:
                    if asyncio.iscoroutinefunction(handler):
                        tasks.append(asyncio.create_task(handler(deepcopy(msg))))
                    else:
                        try:
                            handler(deepcopy(msg))
                        except:
                            pass
                for task in tasks:
                    try:
                        await task
                    except:
                        pass
            except:
                logger.error('\n' + traceback.format_exc())

    async def _ws_manager(self):
        # 心跳处理
        asyncio.create_task(self._pong())
        while not self._exiting:
            # 等待需要更新连接的信号
            await self._update_ws_event.wait()
            # 更新连接
            self._update_ws_task = asyncio.create_task(self._update_ws(self._update_ws_task))
            self._update_ws_event.clear()

    @classmethod
    async def create_instance(cls, apikey, secret):
        self = cls(apikey, secret)
        self._ws_ok = asyncio.get_running_loop().create_future()
        # 启动ws管理器
        asyncio.create_task(self._ws_manager())
        await self._ws_ok
        return self

    def stream_filter(self, _filters: list = None):
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
