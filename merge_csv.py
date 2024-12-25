import os
import pandas as pd
import numpy as np
import re
from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from matplotlib import font_manager

# 한글 폰트 설정 함수
def setup_korean_font():
    """
    GitHub Actions 환경에서 설치된 한글 폰트를 설정합니다.
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
    예: regional-au-weekly-2023-06-15.csv -> AU
    """
    parts = file_name.split('-')
    if len(parts) > 1:
        return parts[1].upper()
    return "UNKNOWN"

def extract_date_from_filename(file_name):
    """
    파일 이름에서 날짜를 추출합니다.
    예: regional-au-weekly-2023-06-15.csv -> 2023-06-15
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

def merge_by_country(input_folder, intermediate_folder, final_output_folder):
    """
    국가별 CSV 파일을 병합하여 단일 데이터셋으로 만듭니다.
    중간 및 최종 결과를 저장합니다.
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
    merged_data['Date'] = pd.to_datetime(merged_data['Date'], errors='coerce')
    merged_data = merged_data.dropna(subset=['Date'])

    # 결측치 처리 및 데이터 정제
    merged_data = merged_data.dropna(subset=['streams'])  # 스트리밍 수 결측치 제거
    merged_data = merged_data[merged_data['streams'] > 0]  # 스트리밍 수가 0인 데이터 제거

    merged_data.to_csv(os.path.join(final_output_folder, "final_merged_data.csv"), index=False, encoding='utf-8-sig')
    print("데이터 병합 및 저장 완료.")

def analyze_music_trends(final_output_folder):
    """
    음악 스트리밍 데이터를 분석하여 트렌드를 도출합니다.
    분석 결과를 저장하고 시각화합니다.
    """
    final_data_path = os.path.join(final_output_folder, "final_merged_data.csv")
    data = pd.read_csv(final_data_path)
    data['Date'] = pd.to_datetime(data['Date'])
    data['Month'] = data['Date'].dt.to_period('M')

    # 1. 국가별 월별 스트리밍 변화율 분석
    country_monthly_streams = data.groupby(['Country', 'Month'])['streams'].sum().reset_index()
    country_monthly_streams['change_rate'] = country_monthly_streams.groupby('Country')['streams'].pct_change() * 100
    country_monthly_streams['change_rate'] = country_monthly_streams['change_rate'].round(2)

    # 2. 국가별 스트리밍 패턴 클러스터링
    pivot_table = country_monthly_streams.pivot(index='Month', columns='Country', values='streams').fillna(0)

    # KMeans 클러스터링
    kmeans = KMeans(n_clusters=4, random_state=42).fit(pivot_table.T)
    clusters_kmeans = pd.DataFrame({'Country': pivot_table.columns, 'Cluster_KMeans': kmeans.labels_})

    # DBSCAN 클러스터링
    dbscan = DBSCAN(eps=0.5, min_samples=2, metric='euclidean').fit(pivot_table.T)
    clusters_dbscan = pd.DataFrame({'Country': pivot_table.columns, 'Cluster_DBSCAN': dbscan.labels_})

    clusters = clusters_kmeans.merge(clusters_dbscan, on='Country')
    clusters.to_csv(os.path.join(final_output_folder, "country_clusters.csv"), index=False, encoding='utf-8-sig')

    # PCA를 이용한 시각화
    pca = PCA(n_components=2)
    reduced_data = pca.fit_transform(pivot_table.T)
    plt.figure(figsize=(12, 12))
    for cluster_id in np.unique(kmeans.labels_):
        cluster_points = reduced_data[kmeans.labels_ == cluster_id]
        plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'KMeans 클러스터 {cluster_id}')
    plt.title('국가별 스트리밍 소비 패턴 클러스터링')
    plt.xlabel('소비 경향 차원 1 (스트리밍 패턴 비교)')
    plt.ylabel('소비 경향 차원 2 (스트리밍 유사성)')
    plt.legend()
    plt.savefig(os.path.join(final_output_folder, "kmeans_clustering_visualization.png"), dpi=300, bbox_inches='tight')
    plt.close()

    plt.figure(figsize=(12, 12))
    for cluster_id in np.unique(dbscan.labels_):
        cluster_points = reduced_data[dbscan.labels_ == cluster_id]
        plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'DBSCAN 클러스터 {cluster_id}')
    plt.title('DBSCAN을 이용한 클러스터링')
    plt.xlabel('소비 경향 차원 1 (스트리밍 패턴 비교)')
    plt.ylabel('소비 경향 차원 2 (스트리밍 유사성)')
    plt.legend()
    plt.savefig(os.path.join(final_output_folder, "dbscan_clustering_visualization.png"), dpi=300, bbox_inches='tight')
    plt.close()

    # 클러스터별 평균 스트리밍 분석
    cluster_analysis = pivot_table.T.groupby(kmeans.labels_).agg(['mean', 'std', 'max'])
    cluster_analysis.columns = ['_'.join(col) for col in cluster_analysis.columns]  # 다중 인덱스 제거
    cluster_analysis.to_csv(os.path.join(final_output_folder, "cluster_analysis.csv"), encoding='utf-8-sig')

    # 인사이트 도출
    insights = []
    insights.append("# 클러스터 분석 결과")
    insights.append("\n## KMeans 기반 클러스터")
    for cluster_id in sorted(clusters_kmeans['Cluster_KMeans'].unique()):
        countries = clusters_kmeans[clusters_kmeans['Cluster_KMeans'] == cluster_id]['Country'].tolist()
        mean_streams = cluster_analysis.loc[cluster_id, 'streams_mean']
        pattern_desc = f"클러스터 {cluster_id}는 평균 스트리밍 수가 {mean_streams:.2f}이며, 유사한 스트리밍 트렌드를 보이는 국가들로 구성되었습니다."
        insights.append(f"- 클러스터 {cluster_id}: {', '.join(countries)} ({pattern_desc})")
    
    insights.append("\n## DBSCAN 기반 클러스터")
    for cluster_id in sorted(clusters_dbscan['Cluster_DBSCAN'].unique()):
        countries = clusters_dbscan[clusters_dbscan['Cluster_DBSCAN'] == cluster_id]['Country'].tolist()
        insights.append(f"- 클러스터 {cluster_id}: {', '.join(countries)}")

    cluster_summary_path = os.path.join(final_output_folder, "insights.txt")
    with open(cluster_summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(insights))

    print("분석 결과 및 인사이트 저장 완료.")

if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"

    merge_by_country(input_folder, intermediate_folder, final_output_folder)
    analyze_music_trends(final_output_folder)
