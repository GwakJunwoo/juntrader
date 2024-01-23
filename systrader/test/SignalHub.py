from MinChartData import MinChartData
import asyncio
from concurrent.futures import ThreadPoolExecutor

class SignalHub:
    def __init__(self, InMemoryDatabase):
        self._strategies = []
        self._data = None
        self._chart = None
        self.InMemoryDatabase = InMemoryDatabase
        self.MinChartData = MinChartData()

        self.InMemoryDatabase.add_listener(self)

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
        #TODO
        # 틱데이터 주는 것은 나중으로 미루자..
#        self._data = newData
#        await self.notify_strategies(newData)
        chart_dict = await self.MinChartData.set_tick(newData)
        if chart_dict:
            for interval in chart_dict:
                await self.InMemoryDatabase.append_or_update_interval_data(interval, chart_dict[interval])

    async def notify(self):
        # 데이터 변경시 호출될 메서드
        await self.notify_strategies()

#    async def update_real_time_data(self):
#        # 실시간 데이터 업데이트 로직 구현
#        await self.notify_strategies()
    
    async def notify_strategies(self):
        loop = asyncio.get_running_loop()

        with ThreadPoolExecutor() as executor:
            tasks = []

            for strategy in self._strategies:
                if asyncio.iscoroutinefunction(strategy.execute):
                    # 비동기 함수는 await을 사용하여 실행
                    task = asyncio.create_task(strategy.execute())
                else:
                    # 일반 함수는 스레드 풀에서 실행
                    task = loop.run_in_executor(executor, strategy.execute)

                tasks.append(task)

            # 모든 태스크가 완료될 때까지 기다림
            await asyncio.gather(*tasks)

"""    async def notify_strategies(self):
        # 전략에 대한 알림 처리, 인자 없이 execute 호출
        tasks = [strategy.execute() for strategy in self._strategies if asyncio.iscoroutinefunction(strategy.execute)]
        # 코루틴이 아닌 경우에 대한 처리
        tasks += [asyncio.get_running_loop().run_in_executor(None, strategy.execute) for strategy in self._strategies if not asyncio.iscoroutinefunction(strategy.execute)]
        
        await asyncio.gather(*tasks)"""

"""    async def notify_strategies(self, newData):
        # 전략에 대한 알림 처리
        tasks = []
        for strategy in self._strategies:
            if asyncio.iscoroutinefunction(strategy.execute):
                # 전략의 update가 코루틴 함수인 경우
                task = strategy.execute(newData)
            else:
                # 전략의 update가 일반 함수인 경우
                loop = asyncio.get_running_loop()
                task = loop.run_in_executor(None, strategy.execute, newData)
            tasks.append(task)
        await asyncio.gather(*tasks)"""
