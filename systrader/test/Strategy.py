from Data import Data
from abc import ABC, abstractmethod

class IntradayStrategy(ABC):
    def __init__(self):
        self.interested_data_types = set()
        self.initialized = False  # 최초 실행 여부를 추적하는 변수

    def is_interested(self, data_type):
        return data_type in self.interested_data_types

    def update_interested_data_types(self):
        if not self.initialized:
            # 관심 데이터 타입 설정
            self.interested_data_types = self.define_interested_data_types()
            self.initialized = True  # 초기화 완료 상태로 설정

    @abstractmethod
    def define_interested_data_types(self):
        # 각 전략 클래스에서 관심있는 데이터 타입을 정의하기 위해 오버라이드해야 함
        pass

    @abstractmethod
    async def execute(self):
        pass


class MomentumStrategy(IntradayStrategy):
    async def execute(self):
        # 최초 실행 시에만 관심 데이터 타입 업데이트
        self.update_interested_data_types()

        # 클래스 메소드로 데이터 접근
        intraday_data = await Data.intraday()
        price = intraday_data['close'].iloc[-1]

        sma_20_price = Data.SMA(intraday_data['close'], 20).iloc[-1]
        sma_5_close = Data.SMA(await Data.daily()['close'], 5).iloc[-1]

        five_min_close_data = await Data.intraday('5m')
        mean_lastest_5min_close = Data.average(five_min_close_data['close'], 20)

        # 트레이딩 시그널 생성
        if price > sma_20_price and price > sma_5_close:
            if price > mean_lastest_5min_close:
                return 1
        else:
            return 0

    def define_interested_data_types(self):
        # MomentumStrategy가 관심 있는 데이터 타입 정의
        return {'intraday', 'daily', 'intraday_5m'}


class TestStrategy:
    def update(self, data):
        #print(data)
        pass

