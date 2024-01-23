from MinChartData import MinChartData
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

class SignalHub:
    def __init__(self, InMemoryDatabase):
        self._strategies = []
        self._data = None
        self._chart = None
        self.InMemoryDatabase = InMemoryDatabase
        self.MinChartData = MinChartData()

    async def set_params(self, params):
        intervals = params.get('interval', [])
        await self._setup_database(intervals)  # asyncio.run 대신 await 사용
        self.MinChartData.set_params(params)

    async def _setup_database(self, intervals):
        # 비동기적으로 데이터베이스 테이블 생성
        for interval in intervals:
            await self.InMemoryDatabase.create_interval_table(interval)

    def add_strategy(self, strategy):
        # 전략 추가
        self._strategies.append(strategy)

    def remove_strategy(self, strategy):
        # 전략 제거
        self._strategies.remove(strategy)

    async def set_tick(self, newData):
        # 비동기적으로 틱 데이터 처리 및 전략에 알림
        self._data = newData
        await self.notify_strategies(newData)
        chart_dict = await self.MinChartData.set_tick(newData)
        if chart_dict:
            for interval in chart_dict:
                await self.InMemoryDatabase.append_or_update_interval_data(interval, chart_dict[interval])

    async def notify_strategies(self, newData):
        # 전략에 대한 알림 처리
        tasks = []
        for strategy in self._strategies:
            if asyncio.iscoroutinefunction(strategy.update):
                # 전략의 update가 코루틴 함수인 경우
                task = strategy.update(newData)
            else:
                # 전략의 update가 일반 함수인 경우
                loop = asyncio.get_running_loop()
                task = loop.run_in_executor(None, strategy.update, newData)
            tasks.append(task)
        await asyncio.gather(*tasks)
