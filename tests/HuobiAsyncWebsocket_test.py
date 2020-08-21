import asyncio
import json
import os

import asyncUnittest
from asyncUnittest import AsyncTestCase

from HuobiAsyncWebsocket.HuobiAsyncWebsocket import HuobiAsyncWs

if os.path.exists(os.path.join(os.path.dirname(__file__), 'key_secret.json')):
    with open('key_secret.json') as f:
        key_secret = json.load(f)
        test_apikey = key_secret['apikey']
        test_secret = key_secret['secret']
else:
    test_apikey = input('Test apikey:')
    test_secret = input('Test secret:')


class CommonTest(AsyncTestCase):
    enable_test = True
    aws: HuobiAsyncWs = None

    @classmethod
    async def setUpClass(cls):
        cls.aws = await HuobiAsyncWs.create_instance(test_apikey, test_secret)

    @classmethod
    async def tearDownClass(cls) -> None:
        await cls.aws.exit()

    async def test_ping_pong(self):
        n = 3
        last_ping_time = None
        async for ping in self.aws.stream_filter([{'action': 'ping'}]):
            if not (last_ping_time is None):
                self.assertEqual(round(asyncio.get_running_loop().time() - last_ping_time), 20)

            last_ping_time = asyncio.get_running_loop().time()
            n -= 1
            if n <= 0:
                break


class TestPingTimeOut(AsyncTestCase):
    '''
    测试接收ping故意超时，通过清理handler致使心跳超时
    '''
    enable_test = 1
    aws: HuobiAsyncWs = None

    @classmethod
    async def setUpClass(cls):
        cls.aws = await HuobiAsyncWs.create_instance(test_apikey, test_secret)

    @classmethod
    async def tearDownClass(cls) -> None:
        await cls.aws.exit()

    async def test_ping_time_out(self):
        old_raw_ws = type(self).aws._ws
        n = 0
        pingpong_handler = list(type(self).aws._handlers)[0]
        async for ping in self.aws.stream_filter([{'action': 'ping'}]):
            if n == 0:
                self.assertIs(type(self).aws._ws, old_raw_ws)
                # 清理pingpong_handler致使下次心跳肯定超时
                type(self).aws._handlers.remove(pingpong_handler)
            elif n == 1:
                await asyncio.sleep(1)
                # 恢复心跳传递
                type(self).aws._handlers.add(pingpong_handler)
            elif n == 2:
                self.assertIsNot(type(self).aws._ws, old_raw_ws)
            n += 1
            if n >= 4:
                break


if __name__ == '__main__':
    asyncUnittest.run()
