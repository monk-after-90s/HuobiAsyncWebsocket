import asyncio
import json
import os

import asyncUnittest
from asyncUnittest import AsyncTestCase

from HuobiAsyncWebsocket.HuobiAsyncWebsocket_pro import HuobiAsyncWs
import ccxt.async_support as ccxt

if os.path.exists(os.path.join(os.path.dirname(__file__), 'key_secret.json')):
    with open('key_secret.json') as f:
        key_secret = json.load(f)
        test_apikey = key_secret['apikey']
        test_secret = key_secret['secret']
else:
    test_apikey = input('Test apikey，或者在同文件夹下放入文件key_secret.json,包含apikey和secret字典:')
    test_secret = input('Test secret:')


class CommonTest(AsyncTestCase):
    enable_test = 1
    aws: HuobiAsyncWs = None
    huobi: ccxt.huobipro = None

    @classmethod
    async def setUpClass(cls):
        cls.aws = HuobiAsyncWs(test_apikey, test_secret)
        cls.huobi = ccxt.huobipro({
            "apiKey": test_apikey,
            "secret": test_secret,
            "enableRateLimit": True, })

    @classmethod
    async def tearDownClass(cls) -> None:
        huobi_exit_task = asyncio.create_task(cls.huobi.close())
        aws_exit_task = asyncio.create_task(cls.aws.exit())
        try:
            await huobi_exit_task
        except:
            pass
        try:
            await aws_exit_task
        except:
            pass

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

    async def test_all_order_stream(self):
        '''
        正常开单测试

        当订单挂单后：
        {
    "action":"push",
    "ch":"orders#btcusdt",
    "data":
    {
        "orderSize":"2.000000000000000000",
        "orderCreateTime":1583853365586,
        "orderPrice":"77.000000000000000000",
        "type":"sell-limit",
        "orderId":27163533,
        "clientOrderId":"abc123",
        "orderStatus":"submitted",
        "symbol":"btcusdt",
        "eventType":"creation"
    }
}


        当订单成交后：
        {
    "action":"push",
    "ch":"orders#btcusdt",
    "data":
    {
        "tradePrice":"76.000000000000000000",
        "tradeVolume":"1.013157894736842100",
        "tradeId":301,
        "tradeTime":1583854188883,
        "aggressor":true,
        "remainAmt":"0.000000000000000400000000000000000000",
        "orderId":27163536,
        "type":"sell-limit",
        "clientOrderId":"abc123",
        "orderStatus":"filled", #有效值：partial-filled, filled
        "symbol":"btcusdt",
        "eventType":"trade"
    }
}

        当订单被撤销后：
        {
    "action":"push",
    "ch":"orders#btcusdt",
    "data":
    {
        "lastActTime":1583853475406,
        "remainAmt":"2.000000000000000000",
        "orderId":27163533,
        "type":"sell-limit",
        "clientOrderId":"abc123",
        "orderStatus":"canceled",
        "symbol":"btcusdt",
        "eventType":"cancellation"
    }
}



        :return:
        '''
        order_info = {'id': None}
        # 开单
        open_order_task = asyncio.create_task(type(self).huobi.create_order('BTC/USDT', 'limit', 'buy', 0.001, 5000))
        async for msg in type(self).aws.all_order_stream():
            if msg['data']['eventType'] == 'creation' and msg['data']['orderPrice'] == '5000' and \
                    msg['data']['type'] == 'buy-limit' and msg['data']['symbol'] == "btcusdt" and \
                    msg['data']['orderSize'] == '0.001':
                order_info = await open_order_task
                self.assertEqual(str(msg['data']['orderId']), order_info['id'])
                self.assertEqual(order_info['symbol'], 'BTC/USDT')
                # 撤单
                asyncio.create_task(type(self).huobi.cancel_order(order_info['id'], order_info['symbol']))
            elif msg['data']['eventType'] == 'cancellation' and \
                    str(msg['data']['orderId']) == order_info['id'] and msg['data']['orderStatus'] == 'canceled':
                break
        await self._test_huge_order_messages()

    async def _test_huge_order_messages(self):
        '''
        超量订单信息测试

        :return:
        '''
        open_order_tasks = [asyncio.create_task(type(self).huobi.create_order('BTC/USDT', 'limit', 'buy', 0.001, 5000))
                            for _ in range(10)]
        all_order_stream = type(self).aws.all_order_stream()
        n = 0
        async for msg in all_order_stream:
            if msg['data']['eventType'] == 'creation' and msg['data']['orderPrice'] == '5000' and \
                    msg['data']['type'] == 'buy-limit' and msg['data']['symbol'] == "btcusdt" and \
                    msg['data']['orderSize'] == '0.001':
                n += 1
                if n >= 10:
                    break
        [asyncio.create_task(type(self).huobi.cancel_order((await task)['id'], (await task)['symbol']))
         for task in open_order_tasks]
        # {
        #     'action': 'push',
        #     'ch': 'orders#*',
        #     'data': {
        #         'lastActTime': 1629444719402,
        #         'execAmt': '0',
        #         'orderSource': 'spot-api',
        #         'orderPrice': '5000',
        #         'orderSize': '0.001',
        #         'remainAmt': '0.001',
        #         'symbol': 'btcusdt',
        #         'type': 'buy-limit',
        #         'eventType': 'cancellation',
        #         'clientOrderId': '',
        #         'orderStatus': 'canceled',
        #         'orderId': 348351566561874,
        #     },
        # }
        async for msg in all_order_stream:
            if msg['data']['eventType'] == 'cancellation' and msg['data']['orderPrice'] == '5000' and \
                    msg['data']['type'] == 'buy-limit' and msg['data']['symbol'] == "btcusdt" and \
                    msg['data']['orderSize'] == '0.001':
                n -= 1
                if n <= 0:
                    break
        await all_order_stream.close()


class TestPingTimeOut(AsyncTestCase):
    '''
    测试接收ping故意超时，通过清理handler致使心跳超时
    '''
    enable_test = 1
    aws: HuobiAsyncWs = None
    huobi: ccxt.huobipro = None

    @classmethod
    async def setUpClass(cls):
        cls.aws = HuobiAsyncWs(test_apikey, test_secret)
        cls.huobi = ccxt.huobipro({
            "apiKey": test_apikey,
            "secret": test_secret,
            "enableRateLimit": True, })

    @classmethod
    async def tearDownClass(cls) -> None:
        huobi_exit_task = asyncio.create_task(cls.huobi.close())
        aws_exit_task = asyncio.create_task(cls.aws.exit())
        try:
            await huobi_exit_task
        except:
            pass
        try:
            await aws_exit_task
        except:
            pass

    async def test_ping_time_out(self):
        n = 0
        await asyncio.sleep(1)
        pingpong_handler = list(type(self).aws._handlers)[0]
        old_raw_ws = type(self).aws.present_ws
        # 开启订单数据流即刻关上，以发出订阅
        asyncio.create_task(type(self).aws.all_order_stream().close())
        async for ping in self.aws.stream_filter([{'action': 'ping'}]):
            if n == 0:
                self.assertIs(type(self).aws.present_ws, old_raw_ws)
                # 清理pingpong_handler致使下次心跳肯定超时
                type(self).aws._handlers.remove(pingpong_handler)
            elif n == 1:
                await asyncio.sleep(1)
                # 恢复心跳传递
                type(self).aws._handlers.add(pingpong_handler)
            elif n == 2:
                self.assertIsNot(type(self).aws.present_ws, old_raw_ws)
                self.assertTrue(old_raw_ws.closed)
                break

            n += 1
        # 更换ws后的订阅
        order_stream = type(self).aws.stream_filter([{'action': 'push',
                                                      'ch': 'orders#*'}])
        open_order_task = asyncio.create_task(type(self).huobi.create_order('BTC/USDT', 'limit', 'buy', 0.001, 5000))
        async for msg in order_stream:
            if msg['data']['eventType'] == 'creation' and msg['data']['orderPrice'] == '5000' and \
                    msg['data']['type'] == 'buy-limit' and msg['data']['symbol'] == "btcusdt" and \
                    msg['data']['orderSize'] == '0.001':
                order_info = await open_order_task
                self.assertEqual(str(msg['data']['orderId']), order_info['id'])
                self.assertEqual(order_info['symbol'], 'BTC/USDT')
                # 撤单
                asyncio.create_task(type(self).huobi.cancel_order(order_info['id'], order_info['symbol']))
            elif msg['data']['eventType'] == 'cancellation' and \
                    str(msg['data']['orderId']) == order_info['id'] and msg['data']['orderStatus'] == 'canceled':
                break
        await order_stream.close()


if __name__ == '__main__':
    asyncUnittest.run()
