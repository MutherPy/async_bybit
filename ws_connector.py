from typing import Union, Iterable
from stream_manager import NatsBybitStreamManager
import asyncio


class NatsBybitClient:
    def __init__(self, callback, symbols_per_stream):
        self.ws_manager = NatsBybitStreamManager(
            symbols_per_stream=symbols_per_stream,
            normal_callback=self._process_normal_message
        )
        self.callback = callback

        self.breaker = 0

    def kline_stream_sub(self, interval: str, symbol: Union[str, Iterable]):
        topic_tmpl = f"kline.{interval}." + "{symbol}"
        self.ws_manager.subscribe(topic_tmpl, symbol)

    def ticker_stream_sub(self, symbol: Union[str, Iterable]):
        topic_tmpl = "tickers.{symbol}"
        self.ws_manager.subscribe(topic_tmpl, symbol)

    def kline_stream_unsub(self, symbol: Union[str, Iterable]):
        self.ws_manager.unsubscribe(symbol)

    def ticker_stream_unsub(self, symbol: Union[str, Iterable]):
        self.ws_manager.unsubscribe(symbol)

    async def _process_normal_message(self, message):
        try:
            self.callback(message)
            await asyncio.sleep(0)
        except Exception as e:
            print('_process_normal_message', e)
            raise e

    @classmethod
    def start(cls, callback, symbols_per_stream):
        print('start')
        t = cls(callback, symbols_per_stream)
        lst = [
            'BTCUSDT',
            'ETHUSDT',
            'BNBUSDT',
            'BNTUSDT',
            'CETUSUSDT',
            'CFXUSDT',
        ]
        interval = '60'
        t.kline_stream_sub(interval, lst)


# def execute():
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     print(loop)
#     loop.run_until_complete(NatsBybitClient.start(print, 10))
#     loop.run_forever()

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.call_soon(NatsBybitClient.start, print, 4)
loop.run_forever()
