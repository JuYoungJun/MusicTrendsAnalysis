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
        df['country'] = country
        df['date'] = date

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
        merged_data['date'] = pd.to_datetime(merged_data['date'], format="%Y-%m-%d", errors='coerce')
        merged_data = merged_data.dropna(subset=['date'])
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

# 국가별 가장 스트리밍이 많은 월 분석
def analyze_streaming_by_month(data, output_folder):
    """
    국가별로 가장 스트리밍이 많은 월을 분석합니다.
    - 컬럼 구성:
      * country: 국가 코드
      * month: 월 (연-월 형식)
      * total_streams: 스트리밍 총합
    - 활용:
      * 특정 국가의 월별 스트리밍 성수기를 파악하여 마케팅 전략 수립에 활용
    - 계산식:
      * total_streams = 월별 스트리밍 데이터의 합계
    """
    data['month'] = pd.to_datetime(data['date']).dt.to_period('M')
    monthly_streams = data.groupby(['country', 'month']).agg(
        total_streams=('streams', 'sum')
    ).reset_index()

    max_streams_by_country = monthly_streams.loc[monthly_streams.groupby('country')['total_streams'].idxmax()]
    max_streams_by_country.to_csv(os.path.join(output_folder, "max_streams_by_month.csv"), index=False, encoding='utf-8-sig')
    print("국가별 가장 스트리밍이 많은 월 분석 완료.")

# 전 세계적으로 스트리밍이 적은 달 분석
def analyze_global_low_streaming_month(data, output_folder):
    """
    전 세계적으로 스트리밍이 가장 적은 달을 분석합니다.
    - 컬럼 구성:
      * month: 월 (연-월 형식)
      * total_streams: 전 세계 스트리밍 총합
    - 활용:
      * 전 세계적으로 음악 스트리밍이 저조한 시기를 파악하여 원인을 분석
    - 계산식:
      * total_streams = 월별 모든 국가의 스트리밍 데이터 합계
    """
    data['month'] = pd.to_datetime(data['date']).dt.to_period('M')
    global_monthly_streams = data.groupby('month').agg(
        total_streams=('streams', 'sum')
    ).reset_index()

    min_stream_month = global_monthly_streams.loc[global_monthly_streams['total_streams'].idxmin()]
    min_stream_month.to_csv(os.path.join(output_folder, "global_low_stream_month.csv"), index=False, encoding='utf-8-sig')
    print("전 세계적으로 스트리밍이 적은 달 분석 완료.")

# 국가별 고유 인기 아티스트와 글로벌 인기 아티스트 비교
def analyze_unique_and_common_artists(data, output_folder):
    """
    국가별 고유 인기 아티스트와 글로벌 인기 아티스트를 비교합니다.
    - 컬럼 구성:
      * country: 국가 코드
      * unique_artists: 해당 국가의 고유 인기 아티스트 목록
      * global_artists: 모든 국가에서 공통적으로 인기 있는 아티스트 목록
    - 활용:
      * 특정 국가에서만 인기를 끄는 아티스트를 발견하여 지역 맞춤형 마케팅 전략에 활용
      * 글로벌 인기 아티스트를 통해 전 세계적으로 공통된 음악 트렌드를 파악
    - 계산식:
      * global_artists = 모든 국가에서 공통적으로 등장한 아티스트의 집합
      * unique_artists = 특정 국가에서만 등장한 아티스트의 집합
    """
    country_artists = data.groupby('country')['artist_names'].apply(lambda x: set(x)).reset_index()
    country_artists.rename(columns={'artist_names': 'artists'}, inplace=True)

    global_artists = set.intersection(*country_artists['artists'])
    unique_artists_by_country = country_artists.copy()
    unique_artists_by_country['unique_artists'] = unique_artists_by_country['artists'].apply(lambda x: x - global_artists)

    # 저장
    pd.DataFrame({'global_artists': list(global_artists)}).to_csv(os.path.join(output_folder, "global_common_artists.csv"), index=False, encoding='utf-8-sig')
    unique_artists_by_country.drop(columns=['artists'], inplace=True)
    unique_artists_by_country.to_csv(os.path.join(output_folder, "unique_artists_by_country.csv"), index=False, encoding='utf-8-sig')
    print("국가별 고유 인기 아티스트와 글로벌 인기 아티스트 비교 완료.")

# 국가별/년도별/월별 스트리밍 수 분석
def analyze_streaming_by_month_year(data, output_folder):
    """
    국가별, 년도별, 월별 스트리밍 수 분석
    - 컬럼 구성:
      * country: 국가 코드
      * year: 연도
      * month: 월
      * total_streams: 스트리밍 총합
    - 활용:
      * 국가별 특정 월/년도의 스트리밍 트렌드를 확인 가능
      * 음악 시장의 성수기와 비수기를 파악하여 마케팅 전략 수립
    - 계산식:
      * total_streams = 특정 연도와 월에 해당하는 스트리밍 데이터의 합계
    """
    data['year'] = pd.to_datetime(data['date']).dt.year
    data['month'] = pd.to_datetime(data['date']).dt.month

    streams_by_month_year = data.groupby(['country', 'year', 'month']).agg(
        total_streams=('streams', 'sum')
    ).reset_index()

    streams_by_month_year.to_csv(os.path.join(output_folder, "streams_by_month_year.csv"), index=False, encoding='utf-8-sig')
    print("국가별/년도별/월별 스트리밍 수 분석 완료.")

# 월별 상위 스트리밍 곡 및 가수 분석
def analyze_monthly_top_artists_and_tracks(data, output_folder):
    """
    월별 상위 스트리밍 곡 및 가수를 분석합니다.
    - 컬럼 구성:
      * country: 국가 코드
      * month: 월
      * track_name: 곡 이름
      * artist_names: 아티스트 이름
      * total_streams: 곡별 스트리밍 수
    - 활용:
      * 특정 월에 어떤 아티스트와 곡이 인기를 끌었는지 확인
      * 월별 트렌드를 분석하여 특정 이벤트와의 연관성 파악
    - 계산식:
      * total_streams = 특정 월의 곡별 스트리밍 데이터의 합계
    """
    data['month'] = pd.to_datetime(data['date']).dt.to_period('M')

    monthly_top_tracks = data.groupby(['country', 'month', 'track_name', 'artist_names']).agg(
        total_streams=('streams', 'sum')
    ).reset_index()

    top_monthly_tracks = monthly_top_tracks.sort_values(['country', 'month', 'total_streams'], ascending=[True, True, False])
    top_monthly_tracks = monthly_top_tracks.groupby(['country', 'month']).head(5)  # 상위 5곡 추출

    top_monthly_tracks.to_csv(os.path.join(output_folder, "monthly_top_tracks.csv"), index=False, encoding='utf-8-sig')
    print("월별 상위 스트리밍 곡 및 가수 분석 완료.")

# 월별 국가 간 공통 아티스트 및 곡 분석
def analyze_common_artists_and_tracks_by_month(data, output_folder):
    """
    월별 국가 간 공통 아티스트 및 곡을 분석합니다.
    - 컬럼 구성:
      * month: 월 (연-월 형식)
      * common_artists: 월별 국가 간 공통 아티스트 목록
      * common_tracks: 월별 국가 간 공통 곡 목록
    - 활용:
      * 특정 월에 모든 국가에서 공통적으로 인기를 끈 아티스트와 곡을 파악
      * 글로벌 음악 트렌드를 확인하고 분석에 활용
    - 계산식:
      * common_artists = 모든 국가에서 공통적으로 등장한 아티스트의 집합
      * common_tracks = 모든 국가에서 공통적으로 등장한 곡의 집합
    """
    data['month'] = pd.to_datetime(data['date']).dt.to_period('M')

    monthly_artists = data.groupby(['month', 'country'])['artist_names'].apply(lambda x: set(x)).reset_index()
    monthly_tracks = data.groupby(['month', 'country'])['track_name'].apply(lambda x: set(x)).reset_index()

    common_artists = monthly_artists.groupby('month')['artist_names'].apply(lambda x: set.intersection(*x)).reset_index()
    common_tracks = monthly_tracks.groupby('month')['track_name'].apply(lambda x: set.intersection(*x)).reset_index()

    common_artists.rename(columns={'artist_names': 'common_artists'}, inplace=True)
    common_tracks.rename(columns={'track_name': 'common_tracks'}, inplace=True)

    common_data = pd.merge(common_artists, common_tracks, on='month')
    common_data.to_csv(os.path.join(output_folder, "monthly_common_artists_tracks.csv"), index=False, encoding='utf-8-sig')

    print("월별 국가 간 공통 아티스트 및 곡 분석 완료.")

# 아티스트 또는 곡의 성장률 분석
def analyze_growth_rate_of_artists_and_tracks(data, output_folder):
    """
    아티스트 또는 곡의 성장률을 분석합니다.
    - 컬럼 구성:
      * country: 국가 코드
      * track_name: 곡 이름
      * artist_names: 아티스트 이름
      * date: 날짜 (주별)
      * streams: 주간 스트리밍 수
      * growth_rate: 스트리밍 성장률 (백분율)
      * trend_status: 성장 또는 감소 추세 상태
    - 활용:
      * 성장 아티스트 발굴: 최근 급격히 성장하는 아티스트를 찾아내고, 해당 아티스트와 협업하거나 프로모션 전략 수립.
      * 성공 유지 전략: 특정 곡이 일정 기간 동안 꾸준히 스트리밍 수를 유지하는지를 확인하여 장기적인 히트곡의 특징 분석.
      * 위기 대응: 스트리밍 수가 감소하는 아티스트나 곡을 식별하고, 감소 원인을 분석하여 대응책 마련.
    - 계산식:
      * growth_rate = ((현재 주 스트리밍 수 - 이전 주 스트리밍 수) / 이전 주 스트리밍 수) * 100
      * trend_status: 성장률(growth_rate)이 5% 이상이면 "Growth", -5% 이하이면 "Decline", 그렇지 않으면 "Stable"
    """
    data['streams'] = data['streams'].fillna(0)
    data = data.sort_values(by=['country', 'artist_names', 'track_name', 'date'])
    data['growth_rate'] = data.groupby(['country', 'artist_names', 'track_name'])['streams'].pct_change().fillna(0) * 100

    def determine_trend_status(growth_rate):
        if growth_rate > 5:
            return "Growth"
        elif growth_rate < -5:
            return "Decline"
        else:
            return "Stable"

    data['trend_status'] = data['growth_rate'].apply(determine_trend_status)

    growth_analysis = data[['country', 'artist_names', 'track_name', 'date', 'streams', 'growth_rate', 'trend_status']]
    growth_analysis.to_csv(os.path.join(output_folder, "growth_rate_analysis.csv"), index=False, encoding='utf-8-sig')

    print("아티스트 또는 곡의 성장률 분석 완료.")

if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"
    merge_by_country(input_folder, intermediate_folder, final_output_folder)
