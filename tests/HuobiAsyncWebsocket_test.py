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


if __name__ == '__main__':
    asyncUnittest.run()
