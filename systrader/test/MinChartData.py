from datetime import datetime
import numpy as np
from numba import jit
import asyncio
from concurrent.futures import ThreadPoolExecutor

class MinChartData:
    def __init__(self):
        self.data = {}
        self.intervals = []
        self.last_returned_time = {}

    def set_params(self, params):
        self.intervals = params.get('interval', [])

    async def set_tick(self, tick_data):
        # 비동기적으로 틱 데이터 처리
        timestamp = datetime.strptime(tick_data['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        code = tick_data['code']
        price = tick_data['price']
        volume = tick_data['volume']

        for interval in self.intervals:
            minute_timestamp = self.adjust_timestamp(timestamp, interval)
            await self.process_tick_for_interval(code, minute_timestamp, price, volume, interval)
        return await self.get_data_to_return(timestamp)

    def adjust_timestamp(self, timestamp, interval):
        # 타임스탬프를 주어진 간격에 맞게 조정
        minutes = int(interval[:-1])
        adjusted_minute = (timestamp.minute // minutes) * minutes
        return timestamp.replace(minute=adjusted_minute, second=0, microsecond=0)

    async def process_tick_for_interval(self, code, minute_timestamp, price, volume, interval):
        # 멀티스레딩을 사용하여 틱 데이터를 각 간격에 따라 처리
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, self._process_tick, code, minute_timestamp, price, volume, interval)

    def _process_tick(self, code, minute_timestamp, price, volume, interval):
        # 틱 데이터 처리 로직
        if code not in self.data:
            self.data[code] = {interval: {}}

        if interval not in self.data[code]:
            self.data[code][interval] = {}

        if minute_timestamp not in self.data[code][interval]:
            self.data[code][interval][minute_timestamp] = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume
            }
        else:
            candle = self.data[code][interval][minute_timestamp]
            candle['high'] = max(candle['high'], price)
            candle['low'] = min(candle['low'], price)
            candle['close'] = price
            candle['volume'] += volume

    async def get_data_to_return(self, current_timestamp):
        data_to_return = {}
        for interval in self.intervals:
            data_to_return_by_interval = []
            interval_start_time = self.adjust_timestamp(current_timestamp, interval)
            if interval_start_time not in self.last_returned_time or self.last_returned_time[interval] < interval_start_time:
                self.last_returned_time[interval] = interval_start_time
                interval_data = await self.get_interval_data(interval, interval_start_time)
                for code, candle in interval_data.items():
                    formatted_data = {
                        'code': code,
                        'timestamp': candle['timestamp'],
                        'open': candle['open'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'close': candle['close'],
                        'volume': candle['volume']
                    }
                    data_to_return_by_interval.append(formatted_data)
                data_to_return[interval] = data_to_return_by_interval
        return data_to_return

    async def get_interval_data(self, interval, end_time):
        interval_data = {}
        for code, candles in self.data.items():
            if end_time in candles[interval]:
                candle = candles[interval][end_time]
                candle_data = {
                    'timestamp': end_time.strftime('%Y-%m-%d %H:%M'),
                    'open': candle['open'],
                    'high': candle['high'],
                    'low': candle['low'],
                    'close': candle['close'],
                    'volume': candle['volume']
                }
                interval_data[code] = candle_data
        return interval_data