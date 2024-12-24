import os
import pandas as pd

# 파일 이름에서 국가 코드 추출
def extract_country_from_filename(file_name):
    """
    파일 이름에서 국가 코드를 추출합니다.
    예: regional-au-weekly-2023-06-15.csv -> AU
    """
    parts = file_name.split('-')
    if len(parts) > 1:
        return parts[1].upper()
    return "UNKNOWN"

# 국가별 상위 스트리밍 곡 분석
def analyze_top_tracks(data):
    """
    국가별로 상위 스트리밍 곡을 분석합니다.
    - 컬럼 구성:
      * country: 국가 코드
      * track_name: 곡 이름
      * artist_names: 아티스트 이름
      * total_streams: 총 스트리밍 수
      * peak_rank: 최고 순위
    """
    top_tracks = data.groupby(['country', 'track_name', 'artist_names']).agg(
        total_streams=('streams', 'sum'),
        peak_rank=('peak_rank', 'min')
    ).reset_index()
    top_tracks = top_tracks.sort_values(['country', 'total_streams'], ascending=[True, False])
    return top_tracks.groupby('country').head(10)

# 최근 차트 상승 곡 분석
def detect_rising_trends(data):
    """
    최근 주차 데이터에서 상승 곡을 분석합니다.
    - 컬럼 구성:
      * country: 국가 코드
      * track_name: 곡 이름
      * artist_names: 아티스트 이름
      * rank_change: 순위 상승폭
      * previous_rank: 이전 주 순위
      * current_rank: 현재 주 순위
    """
    data['rank_change'] = data['previous_rank'] - data['rank']
    rising_trends = data.sort_values(['country', 'rank_change'], ascending=[True, False])
    return rising_trends.groupby('country').head(10)

# 차트 롱런 곡 분석
def analyze_longevity(data):
    """
    차트에서 가장 오래 머문 곡을 분석합니다.
    - 컬럼 구성:
      * country: 국가 코드
      * track_name: 곡 이름
      * artist_names: 아티스트 이름
      * total_weeks: 차트에 머문 총 주 수
      * total_streams: 총 스트리밍 수
    """
    longevity = data.groupby(['country', 'track_name', 'artist_names']).agg(
        total_weeks=('weeks_on_chart', 'sum'),
        total_streams=('streams', 'sum')
    ).reset_index()
    longevity = longevity.sort_values(['country', 'total_weeks'], ascending=[True, False])
    return longevity.groupby('country').head(10)

# 국가별 스트리밍 분포 분석
def analyze_streams_distribution(data):
    """
    국가별 총 스트리밍 합계와 상위 10위 곡의 비율을 계산합니다.
    - 컬럼 구성:
      * country: 국가 코드
      * total_streams: 국가 전체 스트리밍 합계
      * top_10_streams: 상위 10위 곡의 총 스트리밍 수
      * top_10_share: 상위 10위 곡의 비율 (전체 대비 %)
    """
    streams_summary = data.groupby('country').agg(
        total_streams=('streams', 'sum')
    ).reset_index()
    top_10_streams = data[data['rank'] <= 10].groupby('country').agg(
        top_10_streams=('streams', 'sum')
    ).reset_index()
    distribution = pd.merge(streams_summary, top_10_streams, on='country', how='left')
    distribution['top_10_share'] = (distribution['top_10_streams'] / distribution['total_streams']).fillna(0)
    return distribution

# 데이터 병합 및 분석
def merge_by_country(input_folder, intermediate_folder, final_output_folder):
    """
    1. 국가별 CSV 파일 병합
    2. 병합된 데이터를 기반으로 다양한 트렌드 분석 실행
    """
    csv_files = []
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))

    # 병합 데이터 저장용
    country_data = {}

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        country = extract_country_from_filename(file_name)
        df = pd.read_csv(file_path)
        df['country'] = country

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
    merged_data.to_csv(os.path.join(final_output_folder, "final_merged_data.csv"), index=False, encoding='utf-8-sig')

    analyze_top_tracks(merged_data).to_csv(os.path.join(final_output_folder, "top_tracks.csv"), index=False, encoding='utf-8-sig')
    detect_rising_trends(merged_data).to_csv(os.path.join(final_output_folder, "rising_trends.csv"), index=False, encoding='utf-8-sig')
    analyze_longevity(merged_data).to_csv(os.path.join(final_output_folder, "longevity.csv"), index=False, encoding='utf-8-sig')
    analyze_streams_distribution(merged_data).to_csv(os.path.join(final_output_folder, "streams_distribution.csv"), index=False, encoding='utf-8-sig')

    print("병합 및 분석이 완료되었습니다.")

if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"
    merge_by_country(input_folder, intermediate_folder, final_output_folder)
