import numpy as np
from numba import jit
from concurrent.futures import ThreadPoolExecutor
import asyncio

class Frame:
    def __init__(self, input_data, interval='1m'):
        self.data = self._process_input_data(input_data) if not isinstance(input_data, dict) else input_data
        self.interval = interval
        asyncio.run(self._fill_missing_timestamps())

    def _process_input_data(self, input_data):
        data_dict = {}
        for record in input_data:
            code, timestamp, *values = record
            if code not in data_dict:
                data_dict[code] = {}
            data_dict[code][timestamp] = values
        return data_dict

    async def _fill_missing_timestamps(self):
        all_codes = list(self.data.keys())
        all_timestamps = sorted(set(ts for code in all_codes for ts in self.data[code]))
        with ThreadPoolExecutor(max_workers=len(all_codes)) as executor:
            await asyncio.gather(*(executor.submit(self._fill_for_code, code, all_timestamps) for code in all_codes))

    @jit
    def _fill_for_code(self, code, all_timestamps):
        last_values = None
        for timestamp in all_timestamps:
            if timestamp not in self.data[code]:
                fill_values = last_values[:-1] if last_values else np.zeros(4)
                fill_values = np.full(4, fill_values[-1])
                fill_values = np.append(fill_values, 0)
                self.data[code][timestamp] = fill_values
            else:
                last_values = self.data[code][timestamp]

    def get_column_data(self, column_name):
        column_index = {'open': 0, 'high': 1, 'low': 2, 'close': 3, 'volume': 4}.get(column_name.lower())
        if column_index is None:
            raise ValueError(f"Column name '{column_name}' is not valid.")
        column_data = {code: {timestamp: values[column_index] for timestamp, values in timestamps.items()}
                       for code, timestamps in self.data.items()}
        return Frame(column_data, self.interval)

    def __mul__(self, other):
        if not isinstance(other, (int, float)):
            raise TypeError("Only scalar multiplication is supported.")
        return Frame({code: {timestamp: [value * other for value in values] for timestamp, values in timestamps.items()}
                      for code, timestamps in self.data.items()}, self.interval)

    def __str__(self):
        header = "Timestamp\t" + "\t".join(sorted(self.data.keys())) + "\n"
        lines = [header]
        all_timestamps = sorted(set(ts for code in self.data for ts in self.data[code]))
        for timestamp in all_timestamps:
            line = [timestamp] + [str(self.data[code].get(timestamp, ['NaN'])[0]) for code in sorted(self.data.keys())]
            lines.append("\t".join(line) + "\n")
        return ''.join(lines)

    def __getitem__(self, key):
        # 특정 컬럼에 대한 데이터를 가진 새로운 Frame 객체를 반환합니다.
        column_index = {'open': 0, 'high': 1, 'low': 2, 'close': 3, 'volume': 4}.get(key.lower())
        if column_index is None:
            raise KeyError(f"Column '{key}' does not exist.")

        column_data = {code: {timestamp: values[column_index] for timestamp, values in timestamps.items()}
                       for code, timestamps in self.data.items()}
        return Frame(column_data, self.interval)

# 예시 사용법
# frame = Frame(input_data)
# print(frame * 1.1)
