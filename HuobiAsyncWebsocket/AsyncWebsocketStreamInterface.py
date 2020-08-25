import asyncio
import traceback
from abc import ABCMeta, abstractmethod
from copy import deepcopy

import beeprint
import websockets
from ensureTaskCanceled import ensureTaskCanceled
from loguru import logger
from NoLossAsyncGenerator import NoLossAsyncGenerator
from AsyncExclusivePeriods import AsyncExclusivePeriod


class AsyncWebsocketStreamInterface(metaclass=ABCMeta):
    '''
    Used in a asyncio event loop.
    '''

    def __init__(self):
        self._wsq: asyncio.Queue = asyncio.Queue()
        self._wsq.present_ws: websockets.WebSocketClientProtocol = None
        self._wsq.previous_ws: websockets.WebSocketClientProtocol = None
        self._exiting = False
        asyncio.create_task(asyncio.shield(self._ws_manager()))
        self._handlers = set()
        # 更换ws连接时期和处理ws数据时期，刚开始是exchanging_ws时期
        AsyncExclusivePeriod.create_obj_periods(self, 'exchanging_ws', 'handing_ws')

    async def _handle_wsq(self):
        '''
        始终使用原生ws连接队列中的最后一个

        :return:
        '''
        _handle_raw_ws_msg_task = None
        while not self._exiting:
            # 拿到最新原生ws连接
            while True:
                self._wsq.previous_ws = self._wsq.present_ws
                self._wsq.present_ws = await self._wsq.get()
                self._wsq.task_done()

                # 关闭之前的ws连接
                if isinstance(self._wsq.previous_ws, websockets.WebSocketClientProtocol):
                    asyncio.create_task(self._wsq.previous_ws.close())
                # 拿完了，得到最后一个
                if self._wsq.qsize() <= 0:
                    if _handle_raw_ws_msg_task:
                        # 删除之前的ws处理任务
                        asyncio.create_task(ensureTaskCanceled(_handle_raw_ws_msg_task))
                    break
            _handle_raw_ws_msg_task = asyncio.create_task(self._handle_raw_ws_msg(self._wsq.present_ws))
            AsyncExclusivePeriod.set_obj_period(self, 'handing_ws')

    async def _handle_raw_ws_msg(self, ws: websockets.WebSocketClientProtocol):
        try:
            async for msg in ws:
                try:
                    # logger.debug(repr(old_ws_update_ws_task))
                    msg = await self._parse_raw_data(msg)
                    logger.debug(
                        '\n' + beeprint.pp(msg, output=False, string_break_enable=False, sort_keys=False))
                    # logger.debug('\n' + repr(self._update_ws_task))
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
        finally:
            logger.debug('Old connection abandoned.')

    @property
    def present_ws(self):
        return self._wsq.present_ws

    async def send(self, msg):
        await self.present_ws.send(msg)

    async def exit(self):
        self._exiting = True
        await asyncio.create_task(self._wsq.present_ws.close())

    async def _ws_manager(self):
        # 启动ws连接队列的消息对接handlers处理任务
        asyncio.create_task(asyncio.shield(self._handle_wsq()))
        initail = True
        while not self._exiting or initail:
            initail = False
            new_ws = await self._create_ws()
            # 更新连接
            self._wsq.put_nowait(new_ws)
            # wait until ws is exchanged.
            await AsyncExclusivePeriod.wait_enter_period(self, 'handing_ws')
            logger.debug('New ws connection opened.')

            # 等待需要更新连接的信号
            await self._when2create_new_ws()
            # 进入更换ws时期
            AsyncExclusivePeriod.set_obj_period(self, 'exchanging_ws')

    def stream_filter(self, _filters: list = None):
        '''
        Filter the ws data stream and push the filtered data copy to the async generator which is returned by the method.
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

    def add_handler(self, function_or_coroutine_funtion):
        self._handlers.add(function_or_coroutine_funtion)

    async def wait_enter_handing_ws(self):
        await AsyncExclusivePeriod.wait_enter_period(self, 'handing_ws')

    @abstractmethod
    async def _parse_raw_data(self, raw_data):
        pass

    @abstractmethod
    async def _create_ws(self):
        '''
        Create a websockets connection.

        :return:websockets ws instance just created.
        '''
        pass

    @abstractmethod
    async def _when2create_new_ws(self):
        '''
        One time check to notice that it is time to exchange the ws.
        :return:
        '''
        pass


if __name__ == '__main__':
    class C(AsyncWebsocketStreamInterface):
        async def create_ws(self):
            pass


    C()
