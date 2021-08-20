import asyncio
import json

import beeprint
from loguru import logger

import websockets

from AsyncWebsocketStreamInterface import AsyncWebsocketStreamInterface
from HuobiAsyncWebsocket.UrlParamsBuilder import create_signature, UrlParamsBuilder


class HuobiAsyncWs(AsyncWebsocketStreamInterface):
    ws_baseurl = 'wss://api-aws.huobi.pro/ws/v2'

    def __init__(self, apikey, secret):
        super(HuobiAsyncWs, self).__init__()
        self._apikey = apikey
        self._secret = secret
        self._subs = set()

    async def _parse_raw_data(self, raw_data):
        return json.loads(raw_data)

    async def _create_ws(self):
        '''
        Create a websockets connection.

        :return:websockets ws instance just created.
        '''
        # 新建ws连接
        ws = await websockets.connect(self.ws_baseurl)

        # 鉴权
        asyncio.create_task(self._authenticate(ws))
        authentication_stream = self.stream_filter([{
            'action': 'req',
            'code': 200,
            'ch': 'auth'
        }])
        try:
            async for msg in ws:
                msg = json.loads(msg)
                print(f'msg={msg}')
                if isinstance(msg, dict) and msg.get('action') == 'req' and \
                        msg.get('code') == 200 and msg.get('ch') == 'auth':
                    break
        finally:
            asyncio.create_task(authentication_stream.close())

        return ws

    async def _authenticate(self, ws):
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
        await ws.send(json.dumps(auth_request))

    async def _when2create_new_ws(self):
        '''
        One time check to notice that it is time to exchange the ws.

        :return:
        '''
        # ws可用时期刚开始
        # 订阅所有订阅记录
        [await task for task in [asyncio.create_task(self.send(sub)) for sub in self._subs]]
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
            except asyncio.TimeoutError:  # 等心跳超时
                logger.debug('Ping timeout.')
                asyncio.create_task(ping_aiter.close())
                break
            else:
                pong = json.dumps({
                    "action": "pong",
                    "data": {
                        "ts": ping['data']['ts']
                    }
                })
                await self.send(pong)
                logger.debug('\n' + beeprint.pp({
                    "action": "pong",
                    "data": {
                        "ts": ping['data']['ts']
                    }
                }, output=False, string_break_enable=False, sort_keys=False))

    async def add_subscription(self, new_sub: dict):
        b_new_sub = json.dumps(new_sub)
        self._subs.add(b_new_sub)
        # 订阅订单信息
        while not self.present_ws:
            await asyncio.sleep(0)
        asyncio.create_task(self.send(b_new_sub))

    def all_order_stream(self):
        '''
        Filter the ws order data stream and push the filtered data to the async generator which is returned by the method.
        Remember to explicitly call the close method of the async generator to close the stream.


        stream=huobiws.order_stream()

        #handle message in one coroutine:
        async for news in stream:
            ...
        #close the stream in another:
        close_task=asyncio.create_task(stream.close())
        ...
        await close_task

        :return:
        '''
        all_orders_sub = {
            "action": "sub",
            "ch": "orders#*"
        }
        asyncio.create_task(self.add_subscription(all_orders_sub))
        return self.stream_filter([{'action': 'push',
                                    'ch': 'orders#*'}])
