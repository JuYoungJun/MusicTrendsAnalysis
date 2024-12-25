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

    # 추가 분석 알고리즘 실행
    analyze_streaming_by_month(merged_data, final_output_folder)
    analyze_global_low_streaming_month(merged_data, final_output_folder)
    analyze_unique_and_common_artists(merged_data, final_output_folder)
    analyze_streaming_by_month_year(merged_data, final_output_folder)
    analyze_monthly_top_artists_and_tracks(merged_data, final_output_folder)
    analyze_common_artists_and_tracks_by_month(merged_data, final_output_folder)
    analyze_growth_rate_of_artists_and_tracks(merged_data, final_output_folder)

    # 인사이트 도출
    generate_insights(final_output_folder)

# 국가별 가장 스트리밍이 많은 월 분석
def analyze_streaming_by_month(data, output_folder):
    """
    국가별로 가장 스트리밍이 많은 월을 분석합니다.
    - 컬럼 구성:
      * Country: 국가 코드
      * Month: 월 (연-월 형식)
      * Total Streams: 스트리밍 총합
    - 활용 방안:
      * 특정 국가의 월별 스트리밍 트렌드를 분석하여 마케팅 및 광고 집행에 적합한 성수기 파악
      * 국가별 음악 소비 패턴 분석을 통해 지역 특화 콘텐츠 제작 가능
    """
    data['Month'] = pd.to_datetime(data['Date']).dt.to_period('M')
    monthly_streams = data.groupby(['Country', 'Month']).agg(
        Total_Streams=('streams', 'sum')
    ).reset_index()

    max_streams_by_country = monthly_streams.loc[monthly_streams.groupby('Country')['Total_Streams'].idxmax()]
    max_streams_by_country.rename(columns={"Total_Streams": "Max_Streams"}, inplace=True)
    max_streams_by_country.to_csv(os.path.join(output_folder, "max_streams_by_month.csv"), index=False, encoding='utf-8-sig')
    print("국가별 가장 스트리밍이 많은 월 분석 완료.")

# 전 세계적으로 스트리밍이 적은 달 분석
def analyze_global_low_streaming_month(data, output_folder):
    """
    전 세계적으로 스트리밍이 가장 적은 달을 분석합니다.
    - 컬럼 구성:
      * Month: 월 (연-월 형식)
      * Total Streams: 전 세계 스트리밍 총합
    - 활용 방안:
      * 글로벌 음악 소비가 저조한 시기를 파악하여 원인을 분석하고, 특정 이벤트나 프로모션 전략 수립 가능
      * 음악 시장의 전반적인 침체 시기를 예측하여 대응 방안을 마련
    """
    data['Month'] = pd.to_datetime(data['Date']).dt.to_period('M')
    global_monthly_streams = data.groupby('Month').agg(
        Total_Streams=('streams', 'sum')
    ).reset_index()

    min_stream_month = global_monthly_streams.loc[global_monthly_streams['Total_Streams'].idxmin()]
    min_stream_month.to_csv(os.path.join(output_folder, "global_low_stream_month.csv"), index=False, encoding='utf-8-sig')
    print("전 세계적으로 스트리밍이 적은 달 분석 완료.")

# 국가별 고유 인기 아티스트와 글로벌 인기 아티스트 비교
def analyze_unique_and_common_artists(data, output_folder):
    """
    국가별 고유 인기 아티스트와 글로벌 인기 아티스트를 비교합니다.
    - 컬럼 구성:
      * Country: 국가 코드
      * Unique Artists: 해당 국가의 고유 인기 아티스트 목록
      * Global Artists: 모든 국가에서 공통적으로 인기 있는 아티스트 목록
    - 활용 방안:
      * 특정 국가에서만 인기를 끄는 아티스트를 발굴하여 지역별 맞춤형 마케팅 전략 수립 가능
      * 전 세계적으로 공통적인 음악 트렌드와 선호도를 분석하여 글로벌 캠페인에 활용
    """
    country_artists = data.groupby('Country')['artist_names'].apply(lambda x: set(x)).reset_index()
    country_artists.rename(columns={'artist_names': 'Artists'}, inplace=True)

    global_artists = set.intersection(*country_artists['Artists'])
    unique_artists_by_country = country_artists.copy()
    unique_artists_by_country['Unique_Artists'] = unique_artists_by_country['Artists'].apply(lambda x: x - global_artists)

    # 저장
    pd.DataFrame({'Global_Artists': list(global_artists)}).to_csv(os.path.join(output_folder, "global_common_artists.csv"), index=False, encoding='utf-8-sig')
    unique_artists_by_country.drop(columns=['Artists'], inplace=True)
    unique_artists_by_country.to_csv(os.path.join(output_folder, "unique_artists_by_country.csv"), index=False, encoding='utf-8-sig')
    print("국가별 고유 인기 아티스트와 글로벌 인기 아티스트 비교 완료.")

# 국가별/년도별/월별 스트리밍 수 분석
def analyze_streaming_by_month_year(data, output_folder):
    """
    국가별, 년도별, 월별 스트리밍 수 분석
    - 컬럼 구성:
      * Country: 국가 코드
      * Year: 연도
      * Month: 월
      * Total Streams: 스트리밍 총합
    - 활용 방안:
      * 특정 시기의 스트리밍 데이터를 분석하여 음악 시장의 성수기와 비수기를 명확히 파악 가능
      * 연도별 스트리밍 성장을 추적하여 음악 소비의 변화 추세를 이해
    """
    data['Year'] = pd.to_datetime(data['Date']).dt.year
    data['Month'] = pd.to_datetime(data['Date']).dt.month

    streams_by_month_year = data.groupby(['Country', 'Year', 'Month']).agg(
        Total_Streams=('streams', 'sum')
    ).reset_index()

    streams_by_month_year.to_csv(os.path.join(output_folder, "streams_by_month_year.csv"), index=False, encoding='utf-8-sig')
    print("국가별/년도별/월별 스트리밍 수 분석 완료.")

# 월별 상위 스트리밍 곡 및 가수 분석
def analyze_monthly_top_artists_and_tracks(data, output_folder):
    """
    월별 상위 스트리밍 곡 및 가수를 분석합니다.
    - 컬럼 구성:
      * Country: 국가 코드
      * Month: 월
      * Track Name: 곡 이름
      * Artist Names: 아티스트 이름
      * Total Streams: 곡별 스트리밍 수
    - 활용 방안:
      * 특정 월에 인기를 끈 곡과 아티스트를 파악하여 음악 산업 내 트렌드 분석에 활용
      * 월별 상위 트랙 데이터를 통해 이벤트 또는 캠페인에 적합한 음악 추천 가능
    """
    data['Month'] = pd.to_datetime(data['Date']).dt.to_period('M')

    monthly_top_tracks = data.groupby(['Country', 'Month', 'track_name', 'artist_names']).agg(
        Total_Streams=('streams', 'sum')
    ).reset_index()

    top_monthly_tracks = monthly_top_tracks.sort_values(['Country', 'Month', 'Total_Streams'], ascending=[True, True, False])
    top_monthly_tracks = monthly_top_tracks.groupby(['Country', 'Month']).head(5)  # 상위 5곡 추출

    top_monthly_tracks.to_csv(os.path.join(output_folder, "monthly_top_tracks.csv"), index=False, encoding='utf-8-sig')
    print("월별 상위 스트리밍 곡 및 가수 분석 완료.")

# 월별 국가 간 공통 아티스트 및 곡 분석
def analyze_common_artists_and_tracks_by_month(data, output_folder):
    """
    월별 국가 간 공통 아티스트 및 곡을 분석합니다.
    - 컬럼 구성:
      * Month: 월 (연-월 형식)
      * Common Artists: 월별 국가 간 공통 아티스트 목록
      * Common Tracks: 월별 국가 간 공통 곡 목록
    - 활용 방안:
      * 모든 국가에서 공통적으로 인기를 끈 곡과 아티스트를 파악하여 글로벌 음악 트렌드 분석
      * 국가 간 공통적인 소비 패턴을 이해하여 전 세계적인 캠페인 전략 수립
    """
    data['Month'] = pd.to_datetime(data['Date']).dt.to_period('M')

    monthly_artists = data.groupby(['Month', 'Country'])['artist_names'].apply(lambda x: set(x)).reset_index()
    monthly_tracks = data.groupby(['Month', 'Country'])['track_name'].apply(lambda x: set(x)).reset_index()

    common_artists = monthly_artists.groupby('Month')['artist_names'].apply(lambda x: set.intersection(*x)).reset_index()
    common_tracks = monthly_tracks.groupby('Month')['track_name'].apply(lambda x: set.intersection(*x)).reset_index()

    common_artists.rename(columns={'artist_names': 'Common_Artists'}, inplace=True)
    common_tracks.rename(columns={'track_name': 'Common_Tracks'}, inplace=True)

    common_data = pd.merge(common_artists, common_tracks, on='Month')
    common_data.to_csv(os.path.join(output_folder, "monthly_common_artists_tracks.csv"), index=False, encoding='utf-8-sig')

    print("월별 국가 간 공통 아티스트 및 곡 분석 완료.")

# 아티스트 또는 곡의 성장률 분석
def analyze_growth_rate_of_artists_and_tracks(data, output_folder):
    """
    아티스트 또는 곡의 성장률을 분석합니다.
    - 컬럼 구성:
      * Country: 국가 코드
      * Track Name: 곡 이름
      * Artist Names: 아티스트 이름
      * Date: 날짜 (주별)
      * Streams: 주간 스트리밍 수
      * Growth Rate: 스트리밍 성장률 (백분율)
      * Trend Status: 성장 또는 감소 추세 상태
    - 활용 방안:
      * 성장하는 아티스트와 곡을 파악하여 적극적인 프로모션 기회를 모색
      * 감소 추세를 보이는 곡의 원인 분석을 통해 개선 전략 수립
      * 특정 기간 동안 스트리밍 데이터의 상승 및 하락 패턴을 이해하여 예측 모델에 활용
    """
    data['streams'] = data['streams'].fillna(0)
    data = data.sort_values(by=['Country', 'artist_names', 'track_name', 'Date'])
    data['Growth_Rate'] = data.groupby(['Country', 'artist_names', 'track_name'])['streams'].pct_change().fillna(0) * 100

    def determine_trend_status(growth_rate):
        if growth_rate > 5:
            return "Growth"
        elif growth_rate < -5:
            return "Decline"
        else:
            return "Stable"

    data['Trend_Status'] = data['Growth_Rate'].apply(determine_trend_status)

    growth_analysis = data[['Country', 'artist_names', 'track_name', 'Date', 'streams', 'Growth_Rate', 'Trend_Status']]
    growth_analysis.to_csv(os.path.join(output_folder, "growth_rate_analysis.csv"), index=False, encoding='utf-8-sig')

    print("아티스트 또는 곡의 성장률 분석 완료.")

# 인사이트 도출
def generate_insights(output_folder):
    """
    모든 분석 결과를 종합하여 주요 인사이트를 도출합니다.
    - 활용 방안:
      * 의사결정자에게 유용한 데이터를 요약 제공
      * 분석 결과를 통해 음악 소비 트렌드와 시장의 주요 특징 파악
    """
    insights = []

    # 국가별 가장 스트리밍이 많은 월
    max_streams_file = os.path.join(output_folder, "max_streams_by_month.csv")
    if os.path.exists(max_streams_file):
        max_streams = pd.read_csv(max_streams_file)
        top_country = max_streams.loc[max_streams['Max_Streams'].idxmax()]
        insights.append(f"스트리밍이 가장 많은 국가는 {top_country['Country']}이며, 가장 많이 스트리밍된 월은 {top_country['Month']}입니다.")

    # 전 세계적으로 스트리밍이 적은 달
    low_streams_file = os.path.join(output_folder, "global_low_stream_month.csv")
    if os.path.exists(low_streams_file):
        low_streams = pd.read_csv(low_streams_file)
        insights.append(f"전 세계적으로 스트리밍이 가장 낮은 달은 {low_streams['Month'].values[0]}입니다.")

    # 글로벌 인기 아티스트
    global_artists_file = os.path.join(output_folder, "global_common_artists.csv")
    if os.path.exists(global_artists_file):
        global_artists = pd.read_csv(global_artists_file)
        insights.append(f"글로벌 공통 인기 아티스트는 {', '.join(global_artists['Global_Artists'].head(5))} 등입니다.")

    # 인사이트 도출
def generate_insights(output_folder):
    """
    모든 분석 결과를 종합하여 주요 인사이트를 도출합니다.
    """
    insights = []

    try:
        max_streams_file = os.path.join(output_folder, "max_streams_by_month.csv")
        if os.path.exists(max_streams_file):
            max_streams = pd.read_csv(max_streams_file)
            top_country = max_streams.loc[max_streams['Max_Streams'].idxmax()]
            insights.append(f"스트리밍이 가장 많은 국가는 {top_country['Country']}이며, 가장 많이 스트리밍된 월은 {top_country['Month']}입니다.")

        low_streams_file = os.path.join(output_folder, "global_low_stream_month.csv")
        if os.path.exists(low_streams_file):
            low_streams = pd.read_csv(low_streams_file)
            insights.append(f"전 세계적으로 스트리밍이 가장 낮은 달은 {low_streams['Month'].values[0]}입니다.")

        global_artists_file = os.path.join(output_folder, "global_common_artists.csv")
        if os.path.exists(global_artists_file):
            global_artists = pd.read_csv(global_artists_file)
            insights.append(f"글로벌 공통 인기 아티스트는 {', '.join(global_artists['Global_Artists'].head(5))} 등입니다.")

        with open(os.path.join(output_folder, "insights_summary.txt"), "w", encoding="utf-8-sig") as f:
            for insight in insights:
                f.write(insight + "\n")

        print("인사이트 도출 완료. 주요 결과가 insights_summary.txt에 저장되었습니다.")
    except Exception as e:
        print(f"인사이트 생성 중 오류 발생: {e}")

if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"
    merge_by_country(input_folder, intermediate_folder, final_output_folder)
