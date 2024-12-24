import os
import pandas as pd
import json
from jinja2 import Template

def extract_country_from_filename(filename):
    """파일명에서 국가 코드를 추출합니다."""
    parts = filename.split('-')
    if len(parts) > 1:
        return parts[1]  # 국가 코드 추출
    return "unknown"

def save_as_json(dataframe, file_path):
    """DataFrame을 JSON 파일로 저장합니다."""
    dataframe.to_json(file_path, orient='records', lines=False, indent=4)

def save_as_html(dataframe, file_path):
    """DataFrame을 보기 좋게 HTML 파일로 저장합니다."""
    html_template = Template('''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Report</title>
        <style>
            table {
                width: 100%;
                border-collapse: collapse;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
        </style>
    </head>
    <body>
        <h1>Data Report</h1>
        {{ table|safe }}
    </body>
    </html>
    ''')
    rendered_html = html_template.render(table=dataframe.to_html(index=False, classes='table table-striped'))
    with open(file_path, 'w') as f:
        f.write(rendered_html)

def merge_by_country(input_folder, intermediate_folder, final_output_folder):
    # 모든 CSV 파일 목록 가져오기
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    # 국가별 데이터 저장용 딕셔너리
    country_data = {}

    # CSV 파일 읽어서 국가별로 분류
    for file in csv_files:
        file_path = os.path.join(input_folder, file)
        country = extract_country_from_filename(file)
        df = pd.read_csv(file_path)

        if country not in country_data:
            country_data[country] = df
        else:
            country_data[country] = pd.concat([country_data[country], df], ignore_index=True)

    # 국가별 데이터 저장
    os.makedirs(intermediate_folder, exist_ok=True)
    os.makedirs(final_output_folder, exist_ok=True)
    for country, data in country_data.items():
        country_csv = os.path.join(intermediate_folder, f"{country}_data.csv")
        country_json = os.path.join(intermediate_folder, f"{country}_data.json")
        country_html = os.path.join(intermediate_folder, f"{country}_data.html")

        # Save as CSV
        data.to_csv(country_csv, index=False)
        # Save as JSON
        save_as_json(data, country_json)
        # Save as HTML
        save_as_html(data, country_html)

        print(f"Saved country data: {country_csv}, {country_json}, {country_html}")

    # 최종 병합
    merged_data = pd.concat(country_data.values(), ignore_index=True)
    merged_data['country'] = merged_data.apply(lambda row: extract_country_from_filename(row['uri']), axis=1)
    final_csv = os.path.join(final_output_folder, "final_merged_data.csv")
    final_json = os.path.join(final_output_folder, "final_merged_data.json")
    final_html = os.path.join(final_output_folder, "final_merged_data.html")

    # Save final merged data
    merged_data.to_csv(final_csv, index=False)
    save_as_json(merged_data, final_json)
    save_as_html(merged_data, final_html)

    print(f"Final merged data saved to {final_csv}, {final_json}, {final_html}")

# 실행 예제
if __name__ == "__main__":
    input_folder = "./spotify_data"  # 원본 데이터 폴더
    intermediate_folder = "./country_data"  # 국가별 데이터 저장 폴더
    final_output_folder = "./final_data"  # 최종 병합 파일 저장 폴더
    merge_by_country(input_folder, intermediate_folder, final_output_folder)
