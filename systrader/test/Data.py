import asyncio
from functools import lru_cache
from Frame import Frame
import numpy as np

class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Data(metaclass=SingletonMeta):
    def __init__(self, database):
        self.database = database
        self.interval = ['1m', '5m']
        # 필요한 초기화 로직

    @lru_cache(maxsize=1024)
    async def tick(self):
        # 실시간 틱 데이터 로딩
        # 예: {code: {timestamp: (price, volume)}}
        # 데이터베이스에서 틱 데이터를 가져옵니다.
        # Lazy Loading을 사용하여 필요할 때 데이터를 로드합니다.
        return await self._load_tick_data()

#    @lru_cache(maxsize=1024)
    async def intraday(self, interval='1m'):
        # 실시간 인트라데이 데이터 로딩
        # 예: {code: {timestamp: ohlcv}}
        if interval not in self.interval: self.interval.append(interval)
        return await self._load_intraday_data(interval)

    @lru_cache(maxsize=1024)
    async def historical(self, interval='1d', start_date=None):
        # 역사적 데이터 로딩
        # 예: {code: {timestamp: ohlcv}}
        return await self._load_historical_data(interval, start_date)

    async def _load_tick_data(self):
        # 틱 데이터 로딩 로직 구현
        pass

    async def _load_intraday_data(self, interval):
        # 인트라데이 데이터 로딩 로직 구현
        return await self.database.get_real_time_chart_data_as_frame(interval)

    async def _load_historical_data(self, interval, start_date):
        # 역사적 데이터 로딩 로직 구현
        pass

    def get_interval(self):
        return self.interval

    # SMA 및 기타 필요한 계산 메소드 추가 가능

    #TODO
    # 테스트해봐야 함
    def SMA(cls, frame, params):
        window_size = params.get('windows', 10)
        sma_frame_data = {}

        for code, timestamps in frame.data.items():
            values = list(timestamps.values())
            sma_values = [np.nanmean(values[max(0, i - window_size):i]) for i in range(1, len(values) + 1)]
            sma_frame_data[code] = dict(zip(timestamps.keys(), sma_values))

        return Frame(sma_frame_data, frame.interval)

    @classmethod
    def mean(cls, frame, params):
        window_size = params.get('windows', 10)
        mean_frame_data = {}

        for code, timestamps in frame.data.items():
            values = list(timestamps.values())
            mean_values = [np.mean(values[max(0, i - window_size):i]) for i in range(1, len(values) + 1)]
            mean_frame_data[code] = dict(zip(timestamps.keys(), mean_values))

        return Frame(mean_frame_data, frame.interval)

    @classmethod
    def EMA(cls, frame, params):
        period = params.get('period', 12)
        ema_frame_data = {}

        for code, timestamps in frame.data.items():
            values = list(timestamps.values())
            ema_values = cls._calculate_ema(values, period)
            ema_frame_data[code] = dict(zip(timestamps.keys(), ema_values))

        return Frame(ema_frame_data, frame.interval)

    @staticmethod
    def _calculate_ema(values, period):
        ema = []
        multiplier = 2 / (period + 1)
        for i, value in enumerate(values):
            if i < period:
                ema.append(np.nan)
                continue
            if i == period:
                initial_ema = np.mean(values[:period])
                ema.append(initial_ema)
            else:
                ema_value = (value - ema[-1]) * multiplier + ema[-1]
                ema.append(ema_value)
        return ema
    
    @classmethod
    def ADX(cls, frame, period=14):
        # ADX 계산 로직
        # 이 메서드는 실제 ADX 계산 방법에 따라 구현해야 합니다.
        pass

    @classmethod
    def ParabolicSAR(cls, frame):
        # 파라볼릭 SAR 계산 로직
        pass

    @classmethod
    def IchimokuCloud(cls, frame):
        # 일목균형표 계산 로직
        pass

    @classmethod
    def CCI(cls, frame, period=20):
        # CCI 계산 로직
        pass

    @classmethod
    def Momentum(cls, frame, period=10):
        # 모멘텀 계산 로직
        pass

    @classmethod
    def RSI(cls, frame, period=14):
        # RSI 계산 로직
        pass