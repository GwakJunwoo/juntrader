import asyncio
from functools import lru_cache

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
