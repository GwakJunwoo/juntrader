import asyncio
from concurrent.futures import ThreadPoolExecutor

class MomentumStrategy:
    def _set_params(self, data):
        self.data = data
        #self.executor = executor  # 멀티스레딩을 위한 Executor

    async def execute(self):
        # 매번 호출 시 최신 인트라데이 데이터를 가져옴
        intraday_data_1 = await self.data.intraday('1m')
        intraday_data_5 = await self.data.intraday('5m')

    
        print(intraday_data_1.window(length=2)['close'])
        print(intraday_data_5.window(length=2)['close'])

#    def start(self):
#        # 비동기 전략 실행을 멀티스레딩 환경에서 처리
#        loop = asyncio.get_event_loop()
#        loop.run_in_executor(self.executor, asyncio.run, self.execute())

class TestStrategy:
    async def execute(self):
        pass