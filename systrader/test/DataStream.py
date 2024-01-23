import asyncio
from BackTestTickDataGenerator import BackTestTickDataGenerator

class DataStream:
    def __init__(self, codes, signal_hub):
        self.signal_hub = signal_hub
        self.codes = codes
        self.tick_generator = BackTestTickDataGenerator(self.codes)
        self.is_streaming = False
        self.streaming_completed = False

    async def start_stream(self):
        if self.is_streaming:
            self.stop_stream()

        self.is_streaming = True
        await self.initialize_generator()  # 'await' 추가
        await self._stream_data()  # 'await' 추가

    async def _stream_data(self):
        while self.is_streaming:
            tick_data = await self.tick_generator.get_next_tick()
            if tick_data is None:
                # 더 이상 처리할 틱 데이터가 없으므로 스트리밍 중단
                self.stop_stream()
                break
            await self.signal_hub.set_tick(tick_data)

    def stop_stream(self):
        self.is_streaming = False
        self.streaming_completed = True

    async def initialize_generator(self):
        # Initialize the tick generator synchronously
        await self.tick_generator.read_and_merge_csvs()

# 예시 사용법
# codes = ['AAPL', 'GOOG']
# signal_hub = SignalHub(InMemoryDatabase())
# data_stream = DataStream(codes, signal_hub)
# asyncio.run(data_stream.start_stream())
