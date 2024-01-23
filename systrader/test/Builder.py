from InMemoryDatabase import InMemoryDatabase
from SignalHub import SignalHub
from DataStream import DataStream
from Data import Data
import asyncio

class Builder:
    def __init__(self, Universe, Strategy):
        self.Universe = Universe
        self.Strategies = Strategy
        self.InMemoryDatabase = InMemoryDatabase()
        self.signal_hub = None
        self.data_stream = None

    async def start(self):
        await self.InMemoryDatabase.initialize_db()
        self.signal_hub = SignalHub(self.InMemoryDatabase)
        await self.signal_hub.set_params({'interval': ['1m', '5m']})

        for strategy in self.Strategies:
            strategy._set_params(Data(self.InMemoryDatabase))
            self.signal_hub.add_strategy(strategy)

        # DataStream 클래스 내부에서 BackTestTickDataGenerator를 비동기적으로 초기화합니다.
        self.data_stream = DataStream(self.Universe, self.signal_hub)
        await self.data_stream.initialize_generator()

        stream_task = asyncio.create_task(self.data_stream.start_stream())
        
        #await self.data_stream.start_stream()
        await stream_task

        # await self.signal_hub.set_params({'interval': Data.get_interval()})

    async def stop(self):
        self.data_stream.stop_stream()

    async def get_real_time_data(self, interval):
        # InMemoryDatabase의 get_real_time_chart_data 메소드 호출
        return await self.InMemoryDatabase.get_real_time_chart_data(interval)


# 예시 사용법
# Universe = ['AAPL', 'GOOG']
# strategies = [TestStrategy()]
# builder = Builder(Universe, strategies)
# builder.start()
# 필요한 경우에 builder.stop() 호출
