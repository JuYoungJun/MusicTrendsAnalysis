import os
import pandas as pd
import json
from jinja2 import Template

# 파일 이름에서 국가 코드 추출
def extract_country_from_filename(file_name):
    """
    파일 이름에서 국가 코드를 추출합니다.
    파일 형식 예: regional-au-weekly-2023-06-15.csv
    """
    parts = file_name.split('-')
    if len(parts) > 1:
        return parts[1].upper()  # 두 번째 부분이 국가 코드 (대문자로 변환)
    return "UNKNOWN"  # 추출할 수 없는 경우 기본값

# DataFrame을 JSON으로 저장
def save_as_json(dataframe, file_path):
    """DataFrame을 JSON 파일로 저장합니다."""
    dataframe.to_json(file_path, orient='records', lines=False, indent=4)

# DataFrame을 HTML로 저장
def save_as_html(dataframe, file_path):
    """
    DataFrame을 보기 좋게 HTML 파일로 저장합니다.
    """
    html_template = Template('''<!DOCTYPE html>
    <html>
    <head>
        <title>Data Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f9f9f9; }
            h1 { text-align: center; color: #333; }
            table { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 16px; }
            th, td { border: 1px solid #ddd; padding: 10px; }
            th { background-color: #4CAF50; color: white; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            tr:hover { background-color: #ddd; }
        </style>
    </head>
    <body>
        <h1>Data Report</h1>
        {{ table|safe }}
    </body>
    </html>
    ''')
    rendered_html = html_template.render(table=dataframe.to_html(index=False, classes='table table-striped'))
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(rendered_html)

# 국가별 상위 스트리밍 곡 분석
def analyze_top_tracks(data):
    """
    - 국가별로 곡 이름과 아티스트별 스트리밍 합계를 계산합니다.
    - 스트리밍 합계 기준으로 정렬 후, 국가별 상위 10곡을 추출합니다.
    """
    top_tracks = data.groupby(['country', 'track_name', 'artist_names'])['streams'].sum().reset_index()
    top_tracks = top_tracks.sort_values(['country', 'streams'], ascending=[True, False])
    return top_tracks.groupby('country').head(10)

# 최근 차트 상승 곡 분석
def detect_rising_trends(data):
    """
    - 최근 주차 데이터(weeks_on_chart <= 4)를 필터링합니다.
    - 이전 랭크 대비 상승폭('rank_change')을 계산하고 정렬합니다.
    """
    data['rank_change'] = data['previous_rank'] - data['rank']
    recent_data = data[data['weeks_on_chart'] <= 4]
    rising_trends = recent_data.sort_values(['country', 'rank_change'], ascending=[True, False])
    rising_trends['rank_change_absolute'] = rising_trends['rank_change'].abs()  # 상승폭 절대값 추가
    return rising_trends[['country', 'track_name', 'artist_names', 'rank', 'previous_rank', 'rank_change', 'rank_change_absolute']].groupby('country').head(10)

# 차트 롱런 곡 분석
def analyze_longevity(data):
    """
    - 곡 이름과 아티스트별로 차트에 머문 주 수 합계를 계산합니다.
    - 주 수 기준으로 정렬 후, 국가별 상위 10곡을 추출합니다.
    """
    longevity = data.groupby(['country', 'track_name', 'artist_names'])['weeks_on_chart'].sum().reset_index()
    longevity = longevity.sort_values(['country', 'weeks_on_chart'], ascending=[True, False])
    longevity['total_weeks'] = longevity['weeks_on_chart']  # 머문 총 주 수 컬럼 추가
    return longevity[['country', 'track_name', 'artist_names', 'weeks_on_chart', 'total_weeks']].groupby('country').head(10)

# 국가별 스트리밍 분포 분석
def analyze_streams_distribution(data):
    """
    - 국가별 총 스트리밍 합계를 계산합니다.
    - 스트리밍 합계 기준으로 정렬합니다.
    """
    streams_summary = data.groupby('country')['streams'].sum().reset_index()
    return streams_summary.sort_values('streams', ascending=False)

# 데이터 병합 및 분석
def merge_by_country(input_folder, intermediate_folder, final_output_folder):
    """
    1. 국가별 CSV 파일 병합
    2. 병합된 데이터를 기반으로 다양한 트렌드 분석 실행
    """
    # 모든 CSV 파일 경로 가져오기
    csv_files = []
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))

    # 국가별 데이터 병합
    country_data = {}
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        country = extract_country_from_filename(file_name)
        df = pd.read_csv(file_path)

        # 필요한 열이 모두 있는지 확인
        required_columns = ['rank', 'uri', 'artist_names', 'track_name', 'source', 
                            'peak_rank', 'previous_rank', 'weeks_on_chart', 'streams']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None  # 누락된 열 추가

        df['country'] = country
        if country not in country_data:
            country_data[country] = df
        else:
            country_data[country] = pd.concat([country_data[country], df], ignore_index=True)

    # 국가별 데이터 저장
    os.makedirs(intermediate_folder, exist_ok=True)
    os.makedirs(final_output_folder, exist_ok=True)
    for country, data in country_data.items():
        country_csv = os.path.join(intermediate_folder, f"{country}_data.csv")
        data.to_csv(country_csv, index=False, encoding='utf-8-sig')

    # 최종 병합
    merged_data = pd.concat(country_data.values(), ignore_index=True)
    merged_data.to_csv(os.path.join(final_output_folder, "final_merged_data.csv"), index=False, encoding='utf-8-sig')

    # 분석 실행
    analyze_top_tracks(merged_data).to_csv(os.path.join(final_output_folder, "top_tracks.csv"), index=False, encoding='utf-8-sig')
    detect_rising_trends(merged_data).to_csv(os.path.join(final_output_folder, "rising_trends.csv"), index=False, encoding='utf-8-sig')
    analyze_longevity(merged_data).to_csv(os.path.join(final_output_folder, "longevity.csv"), index=False, encoding='utf-8-sig')
    analyze_streams_distribution(merged_data).to_csv(os.path.join(final_output_folder, "streams_distribution.csv"), index=False, encoding='utf-8-sig')

    print("병합 및 분석이 완료되었습니다.")

# 실행 예제
if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"
    merge_by_country(input_folder, intermediate_folder, final_output_folder)
