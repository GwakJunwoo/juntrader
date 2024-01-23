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
