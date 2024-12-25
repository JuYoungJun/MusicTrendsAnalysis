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

def save_csv_with_metadata(df, file_path, description):
    """
    데이터프레임을 저장하면서 메타데이터를 별도 파일로 저장합니다.

    Args:
        df (pd.DataFrame): 저장할 데이터프레임
        file_path (str): 저장할 파일 경로
        description (str): 데이터 설명
    """
    metadata_path = file_path.replace('.csv', '_metadata.txt')
    with open(metadata_path, 'w', encoding='utf-8-sig') as f:
        f.write(description)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')

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
        - artist_names: 아티스트 이름

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

        df = pd.read_csv(file_path, low_memory=False)
        if 'artist_names' not in df.columns or 'track_name' not in df.columns:
            print(f"필수 컬럼 누락: {file_name}, 건너뜁니다.")
            continue

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
        description = f"국가 {country}의 스트리밍 데이터"
        save_csv_with_metadata(data, country_csv, description)

    merged_data = pd.concat(country_data.values(), ignore_index=True)

    if 'Date' not in merged_data.columns:
        print("병합된 데이터에서 'Date' 컬럼이 누락되었습니다.")
        print(f"현재 컬럼 목록: {merged_data.columns.tolist()}")
        raise KeyError("'Date' 컬럼이 병합된 데이터에 존재하지 않습니다.")

    merged_data['Date'] = pd.to_datetime(merged_data['Date'], errors='coerce')
    merged_data = merged_data.dropna(subset=['Date'])

    # 결측치 처리 및 데이터 정제
    merged_data = merged_data.dropna(subset=['streams', 'artist_names', 'track_name'])  # 스트리밍 수 및 필수 컬럼 결측치 제거
    merged_data = merged_data[merged_data['streams'] > 0]  # 스트리밍 수가 0인 데이터 제거

    # 이상치 제거 (IQR 기반)
    Q1 = merged_data['streams'].quantile(0.25)
    Q3 = merged_data['streams'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    merged_data = merged_data[(merged_data['streams'] >= lower_bound) & (merged_data['streams'] <= upper_bound)]

    description = "모든 국가의 병합된 스트리밍 데이터"
    save_csv_with_metadata(merged_data, os.path.join(final_output_folder, "final_merged_data.csv"), description)
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
        - artist_names: 아티스트 이름

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
    data = pd.read_csv(final_data_path, low_memory=False)

    if 'Date' not in data.columns:
        print("분석할 데이터에 'Date' 컬럼이 없습니다.")
        print(f"현재 데이터 샘플: {data.head()}")
        print(f"현재 컬럼 목록: {data.columns.tolist()}")
        raise KeyError("'Date' 컬럼이 분석 데이터에 존재하지 않습니다.")

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
    description = "국가별 월별 가장 많이 스트리밍된 월 분석 결과"
    save_csv_with_metadata(max_stream_month, os.path.join(final_output_folder, "max_stream_month.csv"), description)

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
    description = "국가별 월별 가장 적게 스트리밍된 월 분석 결과"
    save_csv_with_metadata(min_stream_month, os.path.join(final_output_folder, "min_stream_month.csv"), description)

    # 3. 특정 국가에서만 많이 등장하는 가수와 공통적으로 많이 등장하는 가수
    # 국가별로 가장 많이 스트리밍된 가수를 식별합니다.
    # 전세계적으로 가장 많이 스트리밍된 가수도 분석하여 글로벌 트렌드를 파악합니다.
    # 
    # 컬럼 설명:
    # - Country: 국가 코드 (top_artists에서 사용)
    # - artist_names: 아티스트 이름
    # - streams: 스트리밍 수
    # 
    # 활용법:
    # - 국가별로 인기 있는 가수와 글로벌 인기 가수를 비교하여 음악 소비 트렌드를 분석할 수 있습니다.
    artist_streams = data.groupby(['Country', 'artist_names'])['streams'].sum().reset_index()
    top_artists = artist_streams.groupby('Country').apply(lambda x: x.nlargest(5, 'streams')).reset_index(drop=True)
    description = "국가별 가장 많이 스트리밍된 아티스트 분석 결과"
    save_csv_with_metadata(top_artists, os.path.join(final_output_folder, "top_artists_by_country.csv"), description)

    global_top_artists = artist_streams.groupby('artist_names')['streams'].sum().reset_index()
    global_top_artists = global_top_artists.nlargest(10, 'streams')
    description = "전세계적으로 가장 많이 스트리밍된 아티스트 분석 결과"
    save_csv_with_metadata(global_top_artists, os.path.join(final_output_folder, "global_top_artists.csv"), description)

    # 4. 월별 국가들 통틀어서 공통적으로 많이 등장하는 곡 분석
    # 월별로 가장 많이 스트리밍된 곡과 해당 가수를 식별합니다.
    # 
    # 컬럼 설명:
    # - Month: 월 정보
    # - track_name: 곡 이름
    # - artist_names: 아티스트 이름
    # - streams: 스트리밍 수
    # 
    # 활용법:
    # - 월별로 전 세계적으로 가장 인기 있는 곡과 가수를 파악하여 시장 영향력을 분석할 수 있습니다.
    track_streams = data.groupby(['Month', 'track_name', 'artist_names'])['streams'].sum().reset_index()
    monthly_common_tracks = track_streams.groupby('Month').apply(lambda x: x.nlargest(10, 'streams')).reset_index(drop=True)
    description = "월별 전세계적으로 가장 많이 스트리밍된 곡과 가수 분석 결과"
    save_csv_with_metadata(monthly_common_tracks, os.path.join(final_output_folder, "monthly_common_tracks.csv"), description)

    # 5. 월별 국가들 통틀어서 공통적으로 많이 등장하는 아티스트 분석
    # 월별로 가장 많이 스트리밍된 아티스트를 식별합니다.
    # 
    # 컬럼 설명:
    # - Month: 월 정보
    # - artist_names: 아티스트 이름
    # - streams: 스트리밍 수
    # 
    # 활용법:
    # - 월별로 글로벌 음악 시장에서 가장 주목받는 아티스트를 분석할 수 있습니다.
    artist_streams_monthly = data.groupby(['Month', 'artist_names'])['streams'].sum().reset_index()
    monthly_common_artists = artist_streams_monthly.groupby('Month').apply(lambda x: x.nlargest(10, 'streams')).reset_index(drop=True)
    description = "월별 전세계적으로 가장 많이 스트리밍된 아티스트 분석 결과"
    save_csv_with_metadata(monthly_common_artists, os.path.join(final_output_folder, "monthly_common_artists.csv"), description)

    # 6. 월별 곡과 아티스트 통합 트렌드 분석
    # 월별로 가장 인기 있는 곡과 아티스트 데이터를 통합하여 저장합니다.
    # 
    # 컬럼 설명:
    # - Month: 월 정보
    # - track_name: 곡 이름
    # - artist_names: 아티스트 이름
    # - streams: 스트리밍 수
    # 
    # 활용법:
    # - 곡과 아티스트를 통합적으로 분석하여 글로벌 트렌드와 시장의 변화를 한눈에 파악할 수 있습니다.
    combined_data = pd.concat([
        monthly_common_tracks.rename(columns={'track_name': 'Name', 'artist_names': 'Artist'}),
        monthly_common_artists.rename(columns={'artist_names': 'Name'}).assign(Artist=lambda x: x['Name'])
    ])
    description = "월별 전세계적으로 가장 많이 스트리밍된 곡과 아티스트 통합 분석 결과"
    save_csv_with_metadata(combined_data, os.path.join(final_output_folder, "monthly_common_tracks_and_artists.csv"), description)

    print("분석 결과 저장 완료.")

if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"

    merge_by_country(input_folder, intermediate_folder, final_output_folder)
    analyze_music_trends(final_output_folder)
