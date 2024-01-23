from Builder import Builder
from Frame_old import Frame
from Strategy import TestStrategy
from concurrent.futures import ThreadPoolExecutor
import asyncio
import sys
import time

async def main():
    start_time = time.time()

    engine = Builder(
        Universe=['C:/Users/lunar/systrader/systrader/test/AAPL.csv',
                  'C:/Users/lunar/systrader/systrader/test/GOOG.csv'],
        Strategy=[TestStrategy()]
    )
    await engine.start()

    # 스트리밍이 완료될 때까지 기다림
    while not engine.data_stream.streaming_completed:
        await asyncio.sleep(1)  # 작은 대기 시간을 주어 CPU 사용을 줄임

    await engine.InMemoryDatabase.list_tables()
    await engine.stop()
    executor = ThreadPoolExecutor()
    executor.shutdown(wait=False)
    # 모든 작업이 완료되었으므로 프로그램 종료
    print("All streaming and processing completed. Program exiting.")
    
    end_time = time.time()
    execution_time = end_time - start_time  # 실행 시간 계산
    print(f"함수 실행 시간: {execution_time} 초")
    sys.exit()

if __name__ == "__main__":
    asyncio.run(main())
