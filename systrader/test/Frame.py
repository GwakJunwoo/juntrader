import numpy as np
from numba import jit
from concurrent.futures import ThreadPoolExecutor
import asyncio

class Frame:
    def __init__(self, input_data, interval='1m'):
        self.data = self._process_input_data(input_data) if not isinstance(input_data, dict) else input_data
        self.interval = interval
        #asyncio.run(self._fill_missing_timestamps())

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
            line = [timestamp]
            for code in sorted(self.data.keys()):
                # 데이터가 없는 경우에 대한 처리
                values = self.data[code].get(timestamp)
                if values is None:
                    line.append('NaN')
                else:
                    # float 객체인 경우 직접 변환
                    line.append(str(values[0] if isinstance(values, list) else values))
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
    
    def _compare_frames(self, other, op):
        # Frame 간 비교
        if not isinstance(other, Frame):
            raise ValueError("Comparison is only supported between Frame objects.")

        result_data = {}
        for code in self.data:
            if code in other.data:
                result_data[code] = {}
                for timestamp in self.data[code]:
                    if timestamp in other.data[code]:
                        self_value = self.data[code][timestamp]
                        other_value = other.data[code][timestamp]
                        result_data[code][timestamp] = 1 if op(self_value, other_value) else 0

        return Frame(result_data, self.interval)

    def _compare_with_scalar(self, scalar, op):
        # Frame과 스칼라 값 비교
        result_data = {}
        for code, timestamps in self.data.items():
            result_data[code] = {timestamp: 1 if op(value, scalar) else 0 for timestamp, value in timestamps.items()}
        return Frame(result_data, self.interval)

    def __lt__(self, other):
        if isinstance(other, Frame):
            return self._compare_frames(other, lambda self_value, other_value: self_value < other_value)
        elif isinstance(other, (int, float)):
            return self._compare_with_scalar(other, lambda x, y: x < y)
        else:
            raise NotImplementedError("Unsupported operand type for <")
        
    def __le__(self, other):
        if isinstance(other, Frame):
            return self._compare_frames(other, lambda self_value, other_value: self_value <= other_value)
        elif isinstance(other, (int, float)):
            return self._compare_with_scalar(other, lambda x, y: x <= y)
        else:
            raise NotImplementedError("Unsupported operand type for <=")

    def __eq__(self, other):
        if isinstance(other, Frame):
            return self._compare_frames(other, lambda self_value, other_value: self_value == other_value)
        elif isinstance(other, (int, float)):
            return self._compare_with_scalar(other, lambda x, y: x == y)
        else:
            raise NotImplementedError("Unsupported operand type for ==")

    def __ne__(self, other):
        if isinstance(other, Frame):
            return self._compare_frames(other, lambda self_value, other_value: self_value != other_value)
        elif isinstance(other, (int, float)):
            return self._compare_with_scalar(other, lambda x, y: x != y)
        else:
            raise NotImplementedError("Unsupported operand type for !=")

    def __gt__(self, other):
        if isinstance(other, Frame):
            return self._compare_frames(other, lambda self_value, other_value: self_value > other_value)
        elif isinstance(other, (int, float)):
            return self._compare_with_scalar(other, lambda x, y: x > y)
        else:
            raise NotImplementedError("Unsupported operand type for >")

    def __ge__(self, other):
        if isinstance(other, Frame):
            return self._compare_frames(other, lambda self_value, other_value: self_value >= other_value)
        elif isinstance(other, (int, float)):
            return self._compare_with_scalar(other, lambda x, y: x >= y)
        else:
            raise NotImplementedError("Unsupported operand type for >=")
    
    def window(self, length=10):
        # 모든 Timestamp를 정렬하여 마지막 'length' 개를 선택
        all_timestamps = sorted(set(ts for code in self.data for ts in self.data[code]))
        selected_timestamps = all_timestamps[-length:]

        # 선택된 Timestamp에 해당하는 데이터만 추출
        windowed_data = {}
        for code in self.data:
            windowed_data[code] = {ts: self.data[code][ts] for ts in selected_timestamps if ts in self.data[code]}

        return Frame(windowed_data, self.interval)

# 예시 사용법
# frame = Frame(input_data)
# print(frame * 1.1)
