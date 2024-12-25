import os
import pandas as pd
import numpy as np
import re
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

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
    음악 스트리밍 데이터를 분석하여 트렌드를 도출합니다.
    - Input: 최종 병합 데이터가 저장된 폴더 경로
    - Output: 분석 결과 출력 및 저장

    분석 항목:
      1. 국가별 월별 스트리밍 변화율 분석
      2. 국가별 유사 음악 소비 패턴 클러스터링
      3. 월별 상위 곡의 지속성 분석
      4. 주요 인사이트 도출 및 시각화 저장
    """
    # 최종 병합 데이터 로드
    final_data_path = os.path.join(final_output_folder, "final_merged_data.csv")
    data = pd.read_csv(final_data_path)

    # 날짜를 datetime 형식으로 변환
    data['Date'] = pd.to_datetime(data['Date'], format="%Y-%m-%d")
    data['Month'] = data['Date'].dt.to_period('M')  # 월 단위로 그룹화하기 위해 Month 열 생성.

    # 1. 국가별 월별 스트리밍 변화율 분석
    country_monthly_streams = data.groupby(['Country', 'Month'])['streams'].sum().reset_index()
    country_monthly_streams['change_rate'] = country_monthly_streams.groupby('Country')['streams'].pct_change() * 100
    country_monthly_streams_path = os.path.join(final_output_folder, "country_monthly_streams_with_rate.csv")
    country_monthly_streams.to_csv(country_monthly_streams_path, index=False, encoding='utf-8-sig')

    # 2. 국가별 유사 음악 소비 패턴 클러스터링
    pivot_table = country_monthly_streams.pivot(index='Month', columns='Country', values='streams').fillna(0)
    kmeans = KMeans(n_clusters=3, random_state=42).fit(pivot_table.T)
    clusters = pd.DataFrame({'Country': pivot_table.columns, 'Cluster': kmeans.labels_})
    clusters_path = os.path.join(final_output_folder, "country_clusters.csv")
    clusters.to_csv(clusters_path, index=False, encoding='utf-8-sig')

    # 클러스터링 시각화 (PCA를 사용하여 2D로 변환)
    pca = PCA(n_components=2)
    reduced_data = pca.fit_transform(pivot_table.T)
    plt.figure(figsize=(12, 6))

    # 원본 데이터 시각화
    plt.subplot(1, 2, 1)
    plt.scatter(reduced_data[:, 0], reduced_data[:, 1], c='blue', label='Original Data', alpha=0.6)
    plt.title('Original Data (Unclustered)')
    plt.xlabel('PCA Component 1')
    plt.ylabel('PCA Component 2')
    plt.legend()

    # 클러스터링된 데이터 시각화
    plt.subplot(1, 2, 2)
    for cluster_id in range(kmeans.n_clusters):
        cluster_points = reduced_data[kmeans.labels_ == cluster_id]
        plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'Cluster {cluster_id}', alpha=0.6)
    plt.title('Clustered Data')
    plt.xlabel('PCA Component 1')
    plt.ylabel('PCA Component 2')
    plt.legend()

    cluster_plot_path = os.path.join(final_output_folder, "country_clusters_visualization.png")
    plt.savefig(cluster_plot_path)
    plt.close()

    # 3. 월별 상위 곡의 지속성 분석
    top_tracks = data.groupby(['Month', 'track_name'])['streams'].sum().reset_index()
    top_tracks = top_tracks.sort_values(['Month', 'streams'], ascending=[True, False])
    top_tracks['rank'] = top_tracks.groupby('Month')['streams'].rank(ascending=False)
    top_tracks = top_tracks[top_tracks['rank'] <= 5]  # 상위 5곡 필터링
    top_tracks_path = os.path.join(final_output_folder, "top_tracks_by_month.csv")
    top_tracks.to_csv(top_tracks_path, index=False, encoding='utf-8-sig')

    # 4. 주요 인사이트 도출 및 저장
    insights = []

    # 국가별 변화율 인사이트
    insights.append("국가별 월별 스트리밍 변화율 분석 결과 저장 경로:")
    insights.append(f"{country_monthly_streams_path}")

    # 클러스터링 인사이트
    cluster_summary = clusters.groupby('Cluster')['Country'].apply(list)
    insights.append("\n국가별 유사 음악 소비 패턴 클러스터링 결과:")
    for cluster, countries in cluster_summary.items():
        insights.append(f"클러스터 {cluster}: {', '.join(countries)}")
    insights.append(f"결과 파일 경로: {clusters_path}")

    # 상위 곡 지속성 인사이트
    insights.append("\n월별 상위 5곡 지속성 분석 결과 저장 경로:")
    insights.append(f"{top_tracks_path}")

    # 클러스터링 시각화 경로
    insights.append("\n국가별 클러스터링 시각화 저장 경로:")
    insights.append(f"{cluster_plot_path}")

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
