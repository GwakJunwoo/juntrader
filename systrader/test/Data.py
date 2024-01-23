import asyncio
from Frame import Frame
from functools import lru_cache
import numpy as np
from numba import jit

class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class Data(metaclass=SingletonMeta):
    database = None  # 클래스 수준의 database 속성

    @classmethod
    def set_database(cls, database_instance):
        cls.database = database_instance

    @classmethod
    @lru_cache(maxsize=1024)
    async def _get_data(cls, interval):
        # 데이터베이스에서 실시간 차트 데이터를 Frame 객체로 가져옵니다.
        frame = await cls.database.get_real_time_chart_data_as_frame(interval)
        return frame

    @classmethod
    async def intraday(cls, freq='1m'):
        # 주어진 빈도(freq)에 대한 인트라데이 차트 데이터를 Frame 객체로 반환합니다.
        return await cls._get_data(freq)

    @staticmethod
    @jit
    def SMA(data, window):
        # Numba를 사용한 JIT 컴파일로 SMA 계산을 최적화
        return np.convolve(data, np.ones(window)/window, mode='valid')

    @staticmethod
    @jit
    def average(data, window):
        # Numba를 사용한 JIT 컴파일로 평균 계산을 최적화
        return np.mean(data[-window:])

# 예시 사용법
# data = Data(database_instance)
# intraday_data = await data.intraday_chart('1m')
# sma_20 = Data.SMA(intraday_data['close'], 20)
# avg_5 = Data.average(intraday_data['close'], 5)
