import os
import pandas as pd
import re

# 파일 이름에서 국가 코드 추출
def extract_country_from_filename(file_name):
    """
    파일 이름에서 국가 코드를 추출합니다.
    예: regional-au-weekly-2023-06-15.csv -> AU
    - Input: 파일 이름 (문자열)
    - Output: 국가 코드 (문자열)
    """
    parts = file_name.split('-')
    if len(parts) > 1:
        return parts[1].upper()
    return "UNKNOWN"

# 파일 이름에서 날짜 추출
def extract_date_from_filename(file_name):
    """
    파일 이름에서 날짜를 추출합니다.
    예: regional-au-weekly-2023-06-15.csv -> 2023-06-15
    - Input: 파일 이름 (문자열)
    - Output: 날짜 (문자열)
    """
    match = re.search(r"\d{4}-\d{2}-\d{2}", file_name)
    if match:
        date_part = match.group(0)
        try:
            # 날짜 검증
            pd.to_datetime(date_part, format="%Y-%m-%d")
            return date_part
        except ValueError:
            return "INVALID_DATE"
    return "UNKNOWN"

# 데이터 병합 및 저장
def merge_by_country(input_folder, intermediate_folder, final_output_folder):
    """
    1. 국가별 CSV 파일 병합
    2. 병합된 데이터를 파일로 저장
    - Input:
      * input_folder: 원본 데이터가 저장된 폴더 경로
      * intermediate_folder: 중간 병합 데이터를 저장할 폴더 경로
      * final_output_folder: 최종 병합 데이터를 저장할 폴더 경로
    - Output: 병합된 CSV 파일 생성 및 저장

    컬럼 설명:
      * rank: 순위
      * uri: 고유 식별자
      * artist_names: 가수 이름
      * track_name: 트랙 이름
      * source: 데이터 출처
      * peak_rank: 최고 순위
      * previous_rank: 이전 순위
      * weeks_on_chart: 차트 유지 주 수
      * streams: 스트리밍 수
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
        date = extract_date_from_filename(file_name)

        if date in ["INVALID_DATE", "UNKNOWN"]:
            print(f"잘못된 날짜 형식 발견: {file_name}, 파일을 건너뜁니다.")
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

    # 날짜 열 검증 및 변환
    try:
        merged_data['Date'] = pd.to_datetime(merged_data['Date'], format="%Y-%m-%d", errors='coerce')
        merged_data = merged_data.dropna(subset=['Date'])
    except Exception as e:
        print(f"날짜 변환 중 오류 발생: {e}")

    merged_data.to_csv(os.path.join(final_output_folder, "final_merged_data.csv"), index=False, encoding='utf-8-sig')

    print("병합 및 저장이 완료되었습니다.")

# 데이터 분석 알고리즘 추가
def analyze_music_trends(final_output_folder):
    """
    국가별 및 전세계 음악 스트리밍 데이터를 분석합니다.
    - Input: 최종 병합 데이터가 저장된 폴더 경로
    - Output: 분석 결과 출력 및 저장

    분석 항목:
      1. 국가별 가장 음악을 많이 스트리밍한 월:
         - 국가별 월간 스트리밍 합계를 계산하여 가장 많이 스트리밍한 월을 확인.
      2. 전세계적으로 음악 스트리밍이 가장 적은 달:
         - 모든 국가 데이터를 통합하여 월간 합계 중 가장 낮은 값을 가진 달을 찾음.
      3. 특정 국가에서만 인기 있는 가수 & 모든 국가에서 공통적으로 인기 있는 가수:
         - 가수가 등장한 국가 수를 계산하여 특정 국가에서만 인기 있는 가수와 모든 국가에서 인기 있는 가수를 비교.
      4. 주요 인사이트 도출:
         - 분석 결과를 기반으로 국가별 및 글로벌 음악 소비 패턴에 대한 인사이트를 텍스트 파일로 저장.
    """
    # 최종 병합 데이터 로드
    final_data_path = os.path.join(final_output_folder, "final_merged_data.csv")
    data = pd.read_csv(final_data_path)

    # 날짜를 datetime 형식으로 변환
    data['Date'] = pd.to_datetime(data['Date'], format="%Y-%m-%d")
    data['Month'] = data['Date'].dt.to_period('M')  # 월 단위로 그룹화하기 위해 Month 열 생성.

    # 1. 국가별 가장 음악을 많이 스트리밍한 월
    country_monthly_streams = data.groupby(['Country', 'Month'])['streams'].sum().reset_index()
    # 기준:
    # - 데이터를 국가 및 월 단위로 그룹화하여 총 스트리밍 수를 계산.
    # 컬럼 구성:
    # - Country: 국가 코드.
    # - Month: 연-월 (예: 2023-06).
    # - streams: 해당 월에 해당 국가에서 스트리밍된 총 수.
    # 활용법:
    # 국가별 월간 음악 소비 데이터를 분석하여 특정 월의 소비 패턴을 파악.

    max_stream_month_per_country = country_monthly_streams.loc[
        country_monthly_streams.groupby('Country')['streams'].idxmax()
    ]
    # 기준:
    # - 각 국가별로 스트리밍 수가 가장 많은 월을 추출.
    # 컬럼 구성:
    # - Country: 국가 코드.
    # - Month: 스트리밍이 가장 많았던 연-월.
    # - streams: 해당 월의 최대 스트리밍 수.
    # 활용법:
    # 특정 국가의 음악 소비가 최고조에 달했던 시점을 분석.

    max_stream_month_per_country_path = os.path.join(final_output_folder, "max_stream_month_per_country.csv")
    max_stream_month_per_country.to_csv(max_stream_month_per_country_path, index=False, encoding='utf-8-sig')

    # 2. 전세계적으로 음악 스트리밍이 가장 적은 달
    global_monthly_streams = data.groupby('Month')['streams'].sum().reset_index()
    # 기준:
    # - 데이터를 월 단위로 그룹화하여 전세계 스트리밍 합계를 계산.
    # 컬럼 구성:
    # - Month: 연-월 (예: 2023-06).
    # - streams: 해당 월에 전세계에서 스트리밍된 총 수.
    # 활용법:
    # 특정 시점에서 전세계 음악 소비 감소의 원인을 분석.

    min_stream_month = global_monthly_streams.loc[global_monthly_streams['streams'].idxmin()]
    # 기준:
    # - 전세계적으로 스트리밍 수가 가장 낮은 달을 식별.
    # 컬럼 구성:
    # - Month: 스트리밍이 가장 적었던 연-월.
    # - streams: 해당 월의 최소 스트리밍 수.
    # 활용법:
    # 특정 월의 소비 패턴을 분석하여 전세계적 트렌드를 이해.

    global_monthly_streams_path = os.path.join(final_output_folder, "global_monthly_streams.csv")
    global_monthly_streams.to_csv(global_monthly_streams_path, index=False, encoding='utf-8-sig')

    # 3. 특정 국가에서만 많이 등장하는 가수 & 모든 국가에서 공통적으로 인기 있는 가수
    artist_country_counts = data.groupby(['artist_names', 'Country']).size().reset_index(name='count')
    # 기준:
    # - 데이터를 가수와 국가 단위로 그룹화하여 각 국가에서 가수의 등장 횟수를 계산.
    # 컬럼 구성:
    # - artist_names: 가수 이름.
    # - Country: 가수가 등장한 국가 코드.
    # - count: 해당 국가에서 가수가 등장한 횟수.
    # 활용법:
    # 특정 가수의 지역적 인기와 글로벌 인기를 비교.

    unique_country_artists = artist_country_counts.groupby('artist_names')['Country'].nunique()
    # 기준:
    # - 각 가수가 등장한 고유 국가의 수를 계산.
    # 컬럼 구성:
    # - artist_names: 가수 이름.
    # - Country: 고유 국가의 수.
    # 활용법:
    # 특정 가수가 전세계적으로 얼마나 많은 국가에서 인기가 있는지를 분석.

    unique_to_one_country = unique_country_artists[unique_country_artists == 1].reset_index()
    # 기준:
    # - 단일 국가에서만 등장한 가수를 식별.
    # 컬럼 구성:
    # - artist_names: 가수 이름.
    # 활용법:
    # 특정 국가에 특화된 음악 소비 트렌드를 분석.

    unique_to_one_country_path = os.path.join(final_output_folder, "unique_to_one_country.csv")
    unique_to_one_country.to_csv(unique_to_one_country_path, index=False, encoding='utf-8-sig')

    all_countries = data['Country'].nunique()
    global_artists = unique_country_artists[unique_country_artists == all_countries].reset_index()
    # 기준:
    # - 모든 국가에서 공통적으로 등장한 가수를 식별.
    # 컬럼 구성:
    # - artist_names: 가수 이름.
    # 활용법:
    # 전세계적으로 공통적으로 인기 있는 가수를 분석하여 글로벌 트렌드를 이해.

    global_artists_path = os.path.join(final_output_folder, "global_artists.csv")
    global_artists.to_csv(global_artists_path, index=False, encoding='utf-8-sig')

    # 4. 주요 인사이트 도출 및 저장
    insights = []
    insights.append("국가별로 스트리밍이 가장 많았던 월:")
    insights.append(f"결과 파일 경로: {max_stream_month_per_country_path}")

    insights.append(f"\n전세계적으로 음악 소비가 가장 낮았던 달: {min_stream_month['Month']}")
    insights.append(f"결과 파일 경로: {global_monthly_streams_path}")

    insights.append(f"\n특정 국가에서만 인기 있는 가수 수: {len(unique_to_one_country)}명")
    insights.append(f"결과 파일 경로: {unique_to_one_country_path}")

    insights.append(f"\n전세계 모든 국가에서 인기 있는 공통 가수 수: {len(global_artists)}명")
    insights.append(f"결과 파일 경로: {global_artists_path}")

    insights_path = os.path.join(final_output_folder, "insights.txt")
    with open(insights_path, "w", encoding='utf-8') as f:
        f.write("\n".join(insights))

    print("\n주요 인사이트:")
    print("\n".join(insights))

if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"

    merge_by_country(input_folder, intermediate_folder, final_output_folder)
    analyze_music_trends(final_output_folder)
