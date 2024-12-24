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
      * avg_streams: 평균 스트리밍 수
    활용:
      - 국가별 가장 인기 있는 곡과 아티스트를 확인할 수 있습니다.
      - 마케팅 캠페인에서 특정 국가의 선호 곡이나 장르를 분석하는 데 유용합니다.
    """
    top_tracks = data.groupby(['country', 'track_name', 'artist_names']).agg(
        total_streams=('streams', 'sum'),
        peak_rank=('peak_rank', 'min'),
        avg_streams=('streams', 'mean')
    ).reset_index()
    top_tracks = top_tracks.sort_values(['country', 'total_streams'], ascending=[True, False])
    top_tracks['total_streams'] = top_tracks['total_streams'].round(2)
    top_tracks['avg_streams'] = top_tracks['avg_streams'].round(2)
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
      * stream_growth_rate: 스트리밍 성장률
    활용:
      - 새로운 트렌드를 발견하여 빠르게 대응할 수 있습니다.
      - 신곡 마케팅 또는 빠르게 성장하는 장르를 분석하는 데 유용합니다.
    """
    recent_data = data[data['weeks_on_chart'] <= 4].copy()
    recent_data['rank_change'] = recent_data['previous_rank'] - recent_data['rank']
    recent_data['stream_growth_rate'] = recent_data.groupby(['country', 'track_name', 'artist_names'])['streams'].pct_change().fillna(0)
    recent_data['stream_growth_rate'] = recent_data['stream_growth_rate'].round(2)
    rising_trends = recent_data.sort_values(['country', 'rank_change', 'stream_growth_rate'], ascending=[True, False, False])
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
      * avg_streams_per_week: 주당 평균 스트리밍 수
    활용:
      - 장기적으로 인기를 끄는 곡이나 아티스트를 확인할 수 있습니다.
      - 지속적인 관심을 받는 곡의 특성을 분석하는 데 유용합니다.
    """
    longevity = data.groupby(['country', 'track_name', 'artist_names']).agg(
        total_weeks=('weeks_on_chart', 'sum'),
        total_streams=('streams', 'sum'),
        avg_streams_per_week=('streams', 'mean')
    ).reset_index()
    longevity = longevity.sort_values(['country', 'total_weeks'], ascending=[True, False])
    longevity['total_streams'] = longevity['total_streams'].round(2)
    longevity['avg_streams_per_week'] = longevity['avg_streams_per_week'].round(2)
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
      * top_10_tracks: 상위 10위 곡 목록
    활용:
      - 특정 국가에서 상위 곡의 집중도를 확인할 수 있습니다.
      - 음악 시장의 다양성을 평가하거나 상위 곡에 대한 의존도를 분석하는 데 유용합니다.
    """
    streams_summary = data.groupby('country').agg(
        total_streams=('streams', 'sum')
    ).reset_index()
    top_10_streams = data[data['rank'] <= 10].groupby('country').agg(
        top_10_streams=('streams', 'sum')
    ).reset_index()
    top_10_tracks = data[data['rank'] <= 10].groupby('country').agg(
        tracks=('track_name', lambda x: ', '.join(x))
    ).reset_index().rename(columns={'tracks': 'top_10_tracks'})
    distribution = pd.merge(streams_summary, top_10_streams, on='country', how='left')
    distribution = pd.merge(distribution, top_10_tracks, on='country', how='left')
    distribution['top_10_share'] = (distribution['top_10_streams'] / distribution['total_streams']).fillna(0)
    distribution['top_10_share'] = (distribution['top_10_share'] * 100).round(2)
    distribution['total_streams'] = distribution['total_streams'].round(2)
    distribution['top_10_streams'] = distribution['top_10_streams'].round(2)
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
