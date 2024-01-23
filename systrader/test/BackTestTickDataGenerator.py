import pandas as pd
from datetime import datetime
import asyncio
from functools import partial

class BackTestTickDataGenerator:
    def __init__(self, file_paths):
        self.data = None
        self.index = 0
        self.prev_timestamp = None
        self.file_paths = file_paths

    async def read_and_merge_csvs(self):
        all_data = []
        for file_path in self.file_paths:
            df = await self.read_csv(file_path)
            df = df.iloc[:1000].copy()
            df['code'] = file_path[-8:-4]
            all_data.append(df)
        merged_df = pd.concat(all_data)
        merged_df.sort_values(by='timestamp', inplace=True)
        self.data = merged_df

    async def read_csv(self, file_path):
        loop = asyncio.get_running_loop()
        # Use partial to pass additional arguments to pd.read_csv
        func = partial(pd.read_csv, file_path, header=None, names=['timestamp', 'price', 'volume'], dtype={'price': float, 'volume': float})
        return await loop.run_in_executor(None, func)
    
    async def get_next_tick(self):
        if self.index < len(self.data):
            row = self.data.iloc[self.index]
            self.index += 1
            current_timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S.%f')

            if self.prev_timestamp is not None:
                delay = (current_timestamp - self.prev_timestamp).total_seconds()
                #await asyncio.sleep(delay)  # 비동기적으로 지연
            
            self.prev_timestamp = current_timestamp
            return row
        return None


# 예시 사용법
# file_paths = ['/path/to/AAPL.csv', '/path/to/GOOG.csv']
# generator = BackTestTickDataGenerator(file_paths)
# asyncio.run(generator.read_and_process_data())
# tick = generator.get_next_tick()
# while tick is not None:
#     print(tick)
#     tick = generator.get_next_tick()