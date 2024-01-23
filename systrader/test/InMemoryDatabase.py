import sys
from Frame import Frame
sys.path.append('c:/users/lunar/anaconda3/envs/py37_32/lib/site-packages')
import aiosqlite

class InMemoryDatabase:
    def __init__(self):
        self.connection = None

    async def initialize_db(self):
        # 데이터베이스 초기화
        self.connection = await aiosqlite.connect(':memory:')

    async def create_table(self, table_name, columns):
        # 비동기적으로 테이블 생성
        column_defs = ', '.join([f"{col_name} {data_type}" for col_name, data_type in columns])
        create_table_query = f"CREATE TABLE {table_name} ({column_defs})"
        await self.connection.execute(create_table_query)
        await self.connection.commit()

    async def create_interval_table(self, interval):
        # 비동기적으로 테이블 생성
        table_name =  f'real_time_{interval}_chart_data'
        await  self.create_table(table_name, [
            ('code', 'TEXT'),
            ('timestamp', 'TEXT'),
            ('open', 'REAL'),
            ('high', 'REAL'),
            ('low', 'REAL'),
            ('close', 'REAL'),
            ('volume', 'INTEGER')
        ])

    async def list_tables(self):
        # 데이터베이스에 있는 모든 테이블 목록과 내용 조회
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        async with self.connection.cursor() as cursor:
            await cursor.execute(query)
            tables = await cursor.fetchall()
            for table in tables:
                table_name = table[0]
                print(f"Table: {table_name}")
                await cursor.execute(f"SELECT * FROM {table_name}")
                rows = await cursor.fetchall()
                for row in rows:
                    print(row)

    async def append_real_time_chart_data(self, chart_data_list):
        # 비동기적으로 차트 데이터 삽입
        try:
            async with self.connection.cursor() as cursor:
                for chart_dict in chart_data_list:
                    await cursor.execute('''
                        INSERT INTO real_time_chart_data (code, timestamp, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (chart_dict['code'], chart_dict['timestamp'], chart_dict['open'], chart_dict['high'], chart_dict['low'], chart_dict['close'], chart_dict['volume']))
                await self.connection.commit()
        except Exception as e:
            print(f"Error during database operation: {e}")

    async def append_or_update_interval_data(self, interval, chart_data):
        table_name = f'real_time_{interval}_chart_data'
        async with self.connection.cursor() as cursor:
            for chart_dict in chart_data:
                # 먼저 execute를 호출하고, 그 다음 fetchone을 호출합니다.
                await cursor.execute(
                    f'SELECT * FROM {table_name} WHERE code = ? AND timestamp = ?',
                    (chart_dict['code'], chart_dict['timestamp'])
                )
                existing = await cursor.fetchone()
                
                if existing:
                    # 기존 레코드 업데이트
                    await cursor.execute(
                        f'UPDATE {table_name} SET high = ?, low = ?, close = ?, volume = ? WHERE code = ? AND timestamp = ?',
                        (
                            max(chart_dict['high'], existing[2]), 
                            min(chart_dict['low'], existing[3]), 
                            chart_dict['close'], 
                            chart_dict['volume'], 
                            chart_dict['code'], 
                            chart_dict['timestamp']
                        )
                    )
                else:
                    # 새 레코드 삽입
                    await cursor.execute(
                        f'INSERT INTO {table_name} (code, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)',
                        (
                            chart_dict['code'], 
                            chart_dict['timestamp'], 
                            chart_dict['open'], 
                            chart_dict['high'], 
                            chart_dict['low'], 
                            chart_dict['close'], 
                            chart_dict['volume']
                        )
                    )
            await self.connection.commit()

#    async def get_real_time_chart_data(self, interval):
#        # 비동기적으로 차트 데이터 조회
#        async with self.connection.cursor() as cursor:
#            await cursor.execute(f'SELECT * FROM real_time_{interval}_chart_data')
#            return await cursor.fetchall()

    async def get_real_time_chart_data_as_frame(self, interval):
        # 비동기적으로 차트 데이터를 Frame 객체로 조회
        async with self.connection.cursor() as cursor:
            await cursor.execute(f'SELECT * FROM real_time_{interval}_chart_data')
            data = await cursor.fetchall()

        # Frame 객체로 변환하여 반환
        return Frame(data)
    
    async def close(self):
        # 데이터베이스 연결 종료
        await self.connection.close()
