import numpy as np
from numba import jit
import asyncio

class Frame:
    def __init__(self, input_data, interval='1m'):
        # 입력 데이터 형식에 따라 처리
        if isinstance(input_data, dict):
            # 데이터가 dict 형식일 경우 바로 사용
            self.data = input_data
        else:
            # 다른 형식일 경우 처리 함수를 통해 변환
            self.data = self._process_input_data(input_data)
        self.interval = interval
        asyncio.run(self._fill_missing_timestamps())

    def _process_input_data(self, input_data):
        # 입력 데이터를 내부 형식으로 변환
        data_dict = {}
        for record in input_data:
            code, timestamp, *values = record
            if code not in data_dict:
                data_dict[code] = {}
            data_dict[code][timestamp] = values
        return data_dict

    async def _fill_missing_timestamps(self):
        # 비동기로 누락된 타임스탬프 채우기
        all_codes = self.data.keys()
        all_timestamps = sorted(set(ts for code in all_codes for ts in self.data[code]))
        await asyncio.gather(*(self._fill_for_code(code, all_timestamps) for code in all_codes))

    async def _fill_for_code(self, code, all_timestamps):
        # 각 코드에 대해 누락된 타임스탬프 채우기
        last_values = None
        for timestamp in all_timestamps:
            if timestamp not in self.data[code]:
                fill_values = last_values[:-1] if last_values else [0] * 4
                fill_values = [fill_values[3]] * 4
                fill_values.append(0)
                self.data[code][timestamp] = fill_values
            last_values = self.data[code][timestamp]

    def get_column_data(self, column_name):
        # 특정 컬럼의 데이터 추출
        column_index = {'open': 0, 'high': 1, 'low': 2, 'close': 3, 'volume': 4}.get(column_name.lower())
        if column_index is None:
            raise ValueError(f"Column name '{column_name}' is not valid.")

        column_data = {}
        for code, timestamps in self.data.items():
            column_data[code] = {timestamp: values[column_index] for timestamp, values in timestamps.items()}
        
        return Frame(column_data, self.interval)

    def __str__(self):
        # 문자열 표현 생성
        header = "Timestamp\t" + "\t".join(sorted(self.data.keys())) + "\n"
        lines = [header]
        all_timestamps = sorted(set(ts for code in self.data for ts in self.data[code]))

        for timestamp in all_timestamps:
            line = [timestamp]
            for code in sorted(self.data.keys()):
                line.append(str(self.data[code].get(timestamp, ['NaN'])[0]))
            lines.append("\t".join(line) + "\n")

        return ''.join(lines)

# 예시 사용법
# frame = Frame(input_data)
# print(frame)
