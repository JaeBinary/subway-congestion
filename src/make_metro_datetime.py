import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import re

CSV_PATH = r"..\data\input\서울교통공사_서울 도시철도 열차운행시각표_20250704.csv"
DB_PATH = r"..\data\input\metro_datetime.db"
BASE_DATE = "2025-07-04"  # 기준 날짜

def convert_to_datetime(time_str, base_date="2025-07-04"):
    """
    24시간 초과 시간을 실제 datetime으로 변환
    
    예시:
        05:30:00 → 2025-07-04 05:30:00
        24:30:00 → 2025-07-05 00:30:00
        25:15:30 → 2025-07-05 01:15:30
    """
    if pd.isna(time_str) or time_str == '':
        return None
    
    # "HH:MM:SS" 파싱
    match = re.match(r'(\d+):(\d+):(\d+)', str(time_str))
    if not match:
        return None
    
    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3))
    
    # 기준 날짜
    base = datetime.strptime(base_date, '%Y-%m-%d')
    
    # 24시간 초과 처리
    days_to_add = hour // 24  # 0 = 당일, 1 = 익일
    actual_hour = hour % 24
    
    # datetime 생성
    result = base + timedelta(days=days_to_add, hours=actual_hour, minutes=minute, seconds=second)
    
    return result

# CSV 읽기
print("="*70)
print("시간 데이터 DATETIME 변환")
print("="*70)

print(f"\n[1단계] CSV 파일 읽기...")
df = pd.read_csv(CSV_PATH, encoding='cp949')
print(f"  ✓ 총 레코드 수: {len(df):,}개")

# 시간 변환
print(f"\n[2단계] DATETIME 변환 중...")
print(f"  기준 날짜: {BASE_DATE}")
print(f"  • 24시 이전 → {BASE_DATE} HH:MM:SS")
print(f"  • 24시 이후 → 2025-07-05 HH:MM:SS")

df['열차도착시간_dt'] = df['열차도착시간'].apply(lambda x: convert_to_datetime(x, BASE_DATE))
df['열차출발시간_dt'] = df['열차출발시간'].apply(lambda x: convert_to_datetime(x, BASE_DATE))

print(f"  ✓ 변환 완료")

# 변환 결과 확인
print(f"\n[3단계] 변환 결과 확인...")

# 24시간 초과 변환 샘플
over_24 = df[df['열차출발시간'].astype(str).str.match(r'^(24|25):', na=False)].head(5)
if not over_24.empty:
    print(f"\n  [24시간 초과 변환 샘플]")
    for idx, row in over_24.iterrows():
        original = row['열차출발시간']
        converted = row['열차출발시간_dt']
        print(f"    {original:10s} → {converted}")

# 일반 시간 변환 샘플
normal = df[~df['열차출발시간'].astype(str).str.match(r'^(24|25):', na=False)].head(3)
print(f"\n  [일반 시간 변환 샘플]")
for idx, row in normal.iterrows():
    original = row['열차출발시간']
    converted = row['열차출발시간_dt']
    print(f"    {original:10s} → {converted}")

# SQLite 저장
print(f"\n[4단계] SQLite DB 저장...")
table_name = "Metro_Line_1_Schedule"

with sqlite3.connect(DB_PATH) as conn:
    # 원본 시간 + DATETIME 둘 다 저장
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    # 검증
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    
    # 날짜별 레코드 수
    cursor.execute(f"""
        SELECT 
            DATE(열차출발시간_dt) as 날짜,
            COUNT(*) as 레코드수
        FROM {table_name}
        WHERE 열차출발시간_dt IS NOT NULL
        GROUP BY DATE(열차출발시간_dt)
    """)
    date_counts = cursor.fetchall()
    
    print(f"  ✓ 테이블 생성: {table_name}")
    print(f"  ✓ 저장된 레코드: {count:,}개")
    print(f"\n  [날짜별 분포]")
    for date, cnt in date_counts:
        print(f"    {date}: {cnt:,}개")

print("\n" + "="*70)
print("✅ DATETIME 변환 완료!")
print("="*70)
print(f"\n📁 생성된 파일: {DB_PATH}")
print(f"📊 컬럼:")
print(f"   • 열차도착시간 (TEXT) - 원본")
print(f"   • 열차도착시간_dt (DATETIME) - 변환")
print(f"   • 열차출발시간 (TEXT) - 원본")
print(f"   • 열차출발시간_dt (DATETIME) - 변환")