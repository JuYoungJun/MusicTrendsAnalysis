import os
import pandas as pd
import json
from jinja2 import Template

# 파일 이름에서 국가 코드 추출
def extract_country_from_filename(file_name):
    parts = file_name.split('-')
    if len(parts) > 1:
        return parts[1].upper()
    return "UNKNOWN"

# DataFrame을 JSON으로 저장
def save_as_json(dataframe, file_path):
    dataframe.to_json(file_path, orient='records', lines=False, indent=4)

# DataFrame을 HTML로 저장
def save_as_html(dataframe, file_path):
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
    data['weeks_in_top_10'] = data['rank'].apply(lambda x: 1 if x <= 10 else 0)
    top_tracks = data.groupby(['country', 'track_name', 'artist_name']).agg(
        total_streams=('streams', 'sum'),
        weeks_in_top_10=('weeks_in_top_10', 'sum'),
        avg_streams=('streams', 'mean')
    ).reset_index()
    top_tracks = top_tracks.sort_values(['country', 'total_streams'], ascending=[True, False])
    return top_tracks.groupby('country').head(10)

# 최근 차트 상승 곡 분석
def detect_rising_trends(data):
    recent_data = data[data['weeks_on_chart'] <= 4].copy()
    recent_data['rank_change'] = recent_data['previous_rank'] - recent_data['rank']
    recent_data['stream_growth_rate'] = recent_data['streams'].pct_change().fillna(0)
    recent_data['trend_score'] = recent_data['rank_change'] * recent_data['stream_growth_rate']
    rising_trends = recent_data.sort_values(['country', 'trend_score'], ascending=[True, False])
    return rising_trends.groupby('country').head(10)

# 차트 롱런 곡 분석
def analyze_longevity(data):
    longevity = data.groupby(['country', 'track_name', 'artist_name']).agg(
        total_weeks=('weeks_on_chart', 'sum'),
        total_streams=('streams', 'sum'),
        avg_streams_per_week=('streams', 'mean')
    ).reset_index()
    longevity = longevity.sort_values(['country', 'total_weeks'], ascending=[True, False])
    return longevity.groupby('country').head(10)

# 국가별 스트리밍 분포 분석
def analyze_streams_distribution(data):
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
    csv_files = []
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))

    required_columns = ['rank', 'track_name', 'artist_name', 'streams', 'weeks_on_chart', 'previous_rank', 'country']
    country_data = {}
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        country = extract_country_from_filename(file_name)
        df = pd.read_csv(file_path)
        for column in required_columns:
            if column not in df.columns:
                df[column] = None
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
