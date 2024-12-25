import os
import pandas as pd
import numpy as np
import re
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from matplotlib import font_manager

# 한글 폰트 설정 함수
def setup_korean_font():
    """
    한글 폰트를 설정하는 함수입니다. 시스템 환경에 설치된 폰트를 기반으로 설정합니다.
    """
    font_path = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"  # GitHub Actions에서 설치된 경로
    if os.path.exists(font_path):
        font_manager.fontManager.addfont(font_path)
        plt.rcParams['font.family'] = font_manager.FontProperties(fname=font_path).get_name()
        plt.rcParams['axes.unicode_minus'] = False
        print(f"한글 폰트 설정 완료: {font_path}")
    else:
        print("폰트를 찾을 수 없습니다. Actions 설정을 확인하세요.")

# 한글 폰트 설정
setup_korean_font()

def extract_country_from_filename(file_name):
    """
    파일 이름에서 국가 코드를 추출합니다.

    Args:
        file_name (str): 파일 이름 (예: regional-au-weekly-2023-06-15.csv)

    Returns:
        str: 국가 코드 (예: AU)
    """
    parts = file_name.split('-')
    if len(parts) > 1:
        return parts[1].upper()
    return "UNKNOWN"

def extract_date_from_filename(file_name):
    """
    파일 이름에서 날짜를 추출합니다.

    Args:
        file_name (str): 파일 이름 (예: regional-au-weekly-2023-06-15.csv)

    Returns:
        str: 날짜 (예: 2023-06-15)
    """
    match = re.search(r"\d{4}-\d{2}-\d{2}", file_name)
    if match:
        date_part = match.group(0)
        try:
            pd.to_datetime(date_part, format="%Y-%m-%d")
            return date_part
        except ValueError:
            return "INVALID_DATE"
    return "UNKNOWN"

def merge_by_country(input_folder, intermediate_folder, final_output_folder):
    """
    국가별 CSV 파일을 병합하여 단일 데이터셋으로 만듭니다.

    Args:
        input_folder (str): 원본 데이터가 저장된 폴더 경로
        intermediate_folder (str): 중간 데이터 저장 폴더 경로
        final_output_folder (str): 최종 데이터 저장 폴더 경로

    컬럼 설명:
        - Country: 국가 코드 (예: AU, US)
        - Date: 날짜 (예: 2023-06-15)
        - streams: 스트리밍 수 (양의 정수값)
        - track_name: 트랙 이름
        - artist_name: 아티스트 이름

    활용법:
        이 함수는 개별 국가의 데이터를 통합하여 결측치를 처리하고, 이상치를 제거하여 최종 데이터 파일로 저장합니다.
    """
    csv_files = []
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))

    country_data = {}
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        country = extract_country_from_filename(file_name)
        date = extract_date_from_filename(file_name)

        if date in ["INVALID_DATE", "UNKNOWN"]:
            print(f"유효하지 않은 날짜 형식: {file_name}, 건너뜁니다.")
            continue

        df = pd.read_csv(file_path)
        df['Country'] = country
        df['Date'] = date

        if country not in country_data:
            country_data[country] = df
        else:
            country_data[country] = pd.concat([country_data[country], df], ignore_index=True)

    os.makedirs(intermediate_folder, exist_ok=True)
    os.makedirs(final_output_folder, exist_ok=True)

    for country, data in country_data.items():
        country_csv = os.path.join(intermediate_folder, f"{country}_data.csv")
        data.to_csv(country_csv, index=False, encoding='utf-8-sig')

    merged_data = pd.concat(country_data.values(), ignore_index=True)
    merged_data['Date'] = pd.to_datetime(merged_data['Date'], errors='coerce')
    merged_data = merged_data.dropna(subset=['Date'])

    # 결측치 처리 및 데이터 정제
    merged_data = merged_data.dropna(subset=['streams'])  # 스트리밍 수 결측치 제거
    merged_data = merged_data[merged_data['streams'] > 0]  # 스트리밍 수가 0인 데이터 제거

    # 이상치 제거 (IQR 기반)
    Q1 = merged_data['streams'].quantile(0.25)
    Q3 = merged_data['streams'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    merged_data = merged_data[(merged_data['streams'] >= lower_bound) & (merged_data['streams'] <= upper_bound)]

    merged_data.to_csv(os.path.join(final_output_folder, "final_merged_data.csv"), index=False, encoding='utf-8-sig')
    print("데이터 병합 및 저장 완료.")

def analyze_music_trends(final_output_folder):
    """
    음악 스트리밍 데이터를 분석하여 트렌드를 도출합니다.

    Args:
        final_output_folder (str): 최종 데이터 저장 폴더 경로

    컬럼 설명:
        - Country: 국가 코드
        - Month: 월별 데이터 그룹화 (예: 2023-06)
        - streams: 스트리밍 횟수
        - track_name: 곡 이름
        - artist_name: 아티스트 이름

    분석 결과:
        1. 국가별 월별 가장 많이 스트리밍된 월 분석
        2. 특정 달에 스트리밍이 가장 적은 국가 분석
        3. 특정 국가에서만 많이 등장하는 가수와 공통적으로 많이 등장하는 가수
        4. 월별 국가들 통틀어서 공통적으로 많이 등장하는 곡 분석
        5. 월별 국가들 통틀어서 공통적으로 많이 등장하는 아티스트 분석
        6. 월별 곡과 아티스트 통합 트렌드 분석

    활용법:
        이 함수는 데이터를 기반으로 여러 국가 및 월별 트렌드를 분석하고 결과를 CSV 파일로 저장합니다.
    """
    final_data_path = os.path.join(final_output_folder, "final_merged_data.csv")
    data = pd.read_csv(final_data_path)
    data['Date'] = pd.to_datetime(data['Date'])
    data['Month'] = data['Date'].dt.to_period('M')

    # 1. 국가별 월별 가장 많이 스트리밍된 월 분석
    # 국가별로 월별 스트리밍 수를 합산하여 가장 많은 스트리밍 수를 기록한 월을 찾습니다.
    # 결과적으로 각 국가별로 가장 인기 있는 월을 알 수 있습니다.
    # 
    # 컬럼 설명:
    # - Country: 국가 코드
    # - Month: 월 정보 (예: 2023-06)
    # - streams: 해당 월의 총 스트리밍 수
    # 
    # 활용법:
    # - 국가별 가장 스트리밍 활동이 많은 월을 파악하여 시장 마케팅 전략에 활용할 수 있습니다.
    monthly_max_streams = data.groupby(['Country', 'Month'])['streams'].sum().reset_index()
    max_stream_month = monthly_max_streams.loc[monthly_max_streams.groupby('Country')['streams'].idxmax()]
    max_stream_month.to_csv(os.path.join(final_output_folder, "max_stream_month.csv"), index=False, encoding='utf-8-sig')

    # 2. 특정 달에 스트리밍이 가장 적은 국가 분석
    # 국가별로 월별 스트리밍 수를 합산하여 가장 적은 스트리밍 수를 기록한 월을 찾습니다.
    # 결과적으로 각 국가에서 스트리밍 활동이 낮은 월을 파악할 수 있습니다.
    # 
    # 컬럼 설명:
    # - Country: 국가 코드
    # - Month: 월 정보
    # - streams: 해당 월의 총 스트리밍 수
    # 
    # 활용법:
    # - 스트리밍이 낮은 시기에 대한 원인을 분석하고, 캠페인 및 활동 계획에 반영할 수 있습니다.
    min_stream_month = monthly_max_streams.loc[monthly_max_streams.groupby('Country')['streams'].idxmin()]
    min_stream_month.to_csv(os.path.join(final_output_folder, "min_stream_month.csv"), index=False, encoding='utf-8-sig')

    # 3. 특정 국가에서만 많이 등장하는 가수와 공통적으로 많이 등장하는 가수
    # 국가별로 가장 많이 스트리밍된 가수를 식별합니다.
    # 전세계적으로 가장 많이 스트리밍된 가수도 분석하여 글로벌 트렌드를 파악합니다.
    # 
    # 컬럼 설명:
    # - Country: 국가 코드 (top_artists에서 사용)
    # - artist_name: 아티스트 이름
    # - streams: 스트리밍 수
    # 
    # 활용법:
    # - 국가별로 인기 있는 가수와 글로벌 인기 가수를 비교하여 음악 소비 트렌드를 분석할 수 있습니다.
    artist_streams = data.groupby(['Country', 'artist_name'])['streams'].sum().reset_index()
    top_artists = artist_streams.groupby('Country').apply(lambda x: x.nlargest(5, 'streams'))
    top_artists.to_csv(os.path.join(final_output_folder, "top_artists_by_country.csv"), index=False, encoding='utf-8-sig')

    # 전세계적으로 가장 많이 스트리밍된 가수를 분석합니다.
    global_top_artists = artist_streams.groupby('artist_name')['streams'].sum().reset_index()
    global_top_artists = global_top_artists.nlargest(10, 'streams')
    global_top_artists.to_csv(os.path.join(final_output_folder, "global_top_artists.csv"), index=False, encoding='utf-8-sig')

    # 4. 월별 국가들 통틀어서 공통적으로 많이 등장하는 곡 분석
    # 월별로 가장 많이 스트리밍된 곡을 식별합니다.
    # 
    # 컬럼 설명:
    # - Month: 월 정보
    # - track_name: 곡 이름
    # - streams: 스트리밍 수
    # 
    # 활용법:
    # - 월별로 전 세계적으로 가장 인기 있는 곡을 파악하여 해당 곡의 시장 영향력을 분석할 수 있습니다.
    track_streams = data.groupby(['Month', 'track_name'])['streams'].sum().reset_index()
    monthly_common_tracks = track_streams.groupby('Month').apply(lambda x: x.nlargest(10, 'streams')).reset_index(drop=True)
    monthly_common_tracks.to_csv(os.path.join(final_output_folder, "monthly_common_tracks.csv"), index=False, encoding='utf-8-sig')

    # 5. 월별 국가들 통틀어서 공통적으로 많이 등장하는 아티스트 분석
    # 월별로 가장 많이 스트리밍된 아티스트를 식별합니다.
    # 
    # 컬럼 설명:
    # - Month: 월 정보
    # - artist_name: 아티스트 이름
    # - streams: 스트리밍 수
    # 
    # 활용법:
    # - 월별로 글로벌 음악 시장에서 가장 주목받는 아티스트를 분석할 수 있습니다.
    artist_streams_monthly = data.groupby(['Month', 'artist_name'])['streams'].sum().reset_index()
    monthly_common_artists = artist_streams_monthly.groupby('Month').apply(lambda x: x.nlargest(10, 'streams')).reset_index(drop=True)
    monthly_common_artists.to_csv(os.path.join(final_output_folder, "monthly_common_artists.csv"), index=False, encoding='utf-8-sig')

    # 6. 월별 곡과 아티스트 통합 트렌드 분석
    # 월별로 가장 인기 있는 곡과 아티스트 데이터를 통합하여 저장합니다.
    # 
    # 컬럼 설명:
    # - Month: 월 정보
    # - Name: 곡 이름 또는 아티스트 이름
    # - Type: 이름의 유형 (Track 또는 Artist)
    # - streams: 스트리밍 수
    # 
    # 활용법:
    # - 곡과 아티스트를 통합적으로 분석하여 글로벌 트렌드와 시장의 변화를 한눈에 파악할 수 있습니다.
    combined_data = pd.concat([
        monthly_common_tracks.assign(Type='Track', Name=monthly_common_tracks['track_name']).drop(columns=['track_name']),
        monthly_common_artists.assign(Type='Artist', Name=monthly_common_artists['artist_name']).drop(columns=['artist_name'])
    ])
    combined_data.to_csv(os.path.join(final_output_folder, "monthly_common_tracks_and_artists.csv"), index=False, encoding='utf-8-sig')

    print("분석 결과 저장 완료.")

if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"

    merge_by_country(input_folder, intermediate_folder, final_output_folder)
    analyze_music_trends(final_output_folder)
