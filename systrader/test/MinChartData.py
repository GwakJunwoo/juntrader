from datetime import datetime, timedelta
import numpy as np
from numba import jit
import debugpy
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
        
        # 빈 간격을 채우는 로직 추가
        for interval in self.intervals:
            await self.fill_empty_intervals(code, interval, timestamp)

        return await self.get_data_to_return(timestamp)

    def adjust_timestamp(self, timestamp, interval):
        # 타임스탬프를 주어진 간격에 맞게 조정
        minutes = int(interval[:-1])
        adjusted_minute = (timestamp.minute // minutes) * minutes
        return timestamp.replace(minute=adjusted_minute, second=0, microsecond=0)

    async def process_tick_for_interval(self, code, minute_timestamp, price, volume, interval):
        debugpy.debug_this_thread()
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
    
    async def fill_empty_intervals(self, code, interval, current_timestamp):
        # 빈 간격을 찾아서 채우는 로직
        last_candle_time = self.get_last_candle_time(code, interval)
        if last_candle_time:
            while last_candle_time < current_timestamp:
                last_candle_time = self.increment_timestamp(last_candle_time, interval)
                if last_candle_time not in self.data[code][interval]:
                    self._fill_with_previous_close(code, last_candle_time, interval)

    def _fill_with_previous_close(self, code, timestamp, interval):
        # 이전 close 가격으로 새 캔들 생성
        previous_candle = self.get_previous_candle(code, interval, timestamp)
        if previous_candle:
            self.data[code][interval][timestamp] = {
                'open': previous_candle['close'],
                'high': previous_candle['close'],
                'low': previous_candle['close'],
                'close': previous_candle['close'],
                'volume': 0
            }

    def get_last_candle_time(self, code, interval):
        # 마지막 캔들의 시간을 반환
        if code in self.data and interval in self.data[code]:
            return max(self.data[code][interval].keys())
        return None

    def get_previous_candle(self, code, interval, timestamp):
        # 주어진 시간 이전의 마지막 캔들을 반환
        sorted_candles = sorted(self.data[code][interval].items())
        for candle_time, candle_data in sorted_candles:
            if candle_time < timestamp:
                return candle_data
        return None

    def increment_timestamp(self, timestamp, interval):
        # 주어진 간격만큼 시간을 증가시키는 함수
        minutes = int(interval[:-1])
        return timestamp + timedelta(minutes=minutes)