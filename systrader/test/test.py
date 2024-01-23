import asyncio
import yappi
from Builder import Builder
from systrader.test.Frame_old import Frame
import pstats
from Strategy import TestStrategy
import pyprof2calltree

async def main():
    engine = Builder(
        Universe=['C:/Users/lunar/systrader/systrader/test/AAPL.csv'],
        Strategy=[TestStrategy()]
    )
    await engine.start()

    # 예를 들어, 10초 동안 스트리밍 후 종료
    await asyncio.sleep(10)
    await engine.stop()

if __name__ == "__main__":
    yappi.start()
    asyncio.run(main())
    yappi.stop()

    # Save the profiling data in pstat format
    yappi.get_func_stats().save('output.pstat', type='pstat')

    # Load the pstat file using pstats.Stats
    stats = pstats.Stats('output.pstat')

    # Convert pstats object to calltree format and save to file
    pyprof2calltree.convert(stats, 'output.callgrind')

    print('Profile converted to callgrind format and saved!')


import asyncio
from InMemoryDatabase import InMemoryDatabase  # 가정된 InMemoryDatabase 모듈
from SignalHub import SignalHub  # 가정된 SignalHub 모듈

async def data_producer(signal_hub):
    while True:
        # 실시간 데이터 업데이트
        await signal_hub.update_real_time_data()
        await asyncio.sleep(1)  # 1초 간격으로 데이터 업데이트

async def main():
    # InMemoryDatabase와 SignalHub 인스턴스 생성
    in_memory_db = InMemoryDatabase()
    await in_memory_db.initialize_db()  # 데이터베이스 초기화
    signal_hub = SignalHub(in_memory_db)

    # 필요한 파라미터 설정
    params = {"interval": ["1m", "5m", "1h"]}
    await signal_hub.set_params(params)

    # 전략 추가 (전략 클래스는 별도로 정의되어야 함)
    # 예: signal_hub.add_strategy(YourStrategy())

    # 데이터 프로듀서 및 신호 허브 실행
    producer_task = asyncio.create_task(data_producer(signal_hub))

    # 기타 필요한 비동기 작업 추가 가능
    # 예: consumer_task = asyncio.create_task(other_async_function())

    # 모든 비동기 작업 기다림
    await producer_task
    # await consumer_task 등 다른 비동기 작업이 있다면 같이 기다립니다.

# 프로그램 실행
if __name__ == '__main__':
    asyncio.run(main())

