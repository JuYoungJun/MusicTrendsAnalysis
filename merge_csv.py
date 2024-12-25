import os
import pandas as pd
import numpy as np
import re
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from matplotlib import font_manager
from matplotlib.backends.backend_pdf import PdfPages

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
    - Input: 파일 이름 (str)
    - Output: 국가 코드 (str)
    """
    parts = file_name.split('-')
    if len(parts) > 1:
        return parts[1].upper()
    return "UNKNOWN"

def extract_date_from_filename(file_name):
    """
    파일 이름에서 날짜를 추출합니다.
    예: regional-au-weekly-2023-06-15.csv -> 2023-06-15
    - Input: 파일 이름 (str)
    - Output: 날짜 (str)
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
    - Input: CSV 파일 경로 (input_folder)
    - Output: 병합된 데이터 저장 (final_output_folder)

    컬럼 설명:
    - Country: 국가 코드
    - Date: 데이터 기록 날짜
    - streams: 스트리밍 횟수
    - 기타 원본 데이터 컬럼 유지
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

def generate_report_pdf(insights, plots, output_path):
    """
    PDF 보고서를 생성합니다.
    - Input:
      - insights: 분석 결과 텍스트
      - plots: 시각화 이미지 경로 목록
      - output_path: PDF 저장 경로
    """
    with PdfPages(output_path) as pdf:
        # 텍스트 페이지 추가
        plt.figure(figsize=(8.5, 11))
        plt.axis('off')
        plt.text(0, 1, "\n".join(insights), fontsize=12, va='top', wrap=True)
        pdf.savefig()
        plt.close()

        # 각 시각화 이미지 추가
        for plot_path in plots:
            img = plt.imread(plot_path)
            plt.figure(figsize=(8.5, 11))
            plt.imshow(img)
            plt.axis('off')
            pdf.savefig()
            plt.close()

def analyze_music_trends(final_output_folder):
    """
    음악 스트리밍 데이터를 분석하여 트렌드를 도출합니다.
    분석 결과를 저장하고 시각화합니다.

    컬럼 설명 및 분석 목적:
    - Country: 국가 코드
    - Month: 월별 데이터 그룹화
    - streams: 월별 스트리밍 횟수
    - change_rate: 월별 스트리밍 변화율
    """
    final_data_path = os.path.join(final_output_folder, "final_merged_data.csv")
    data = pd.read_csv(final_data_path)
    data['Date'] = pd.to_datetime(data['Date'])
    data['Month'] = data['Date'].dt.to_period('M')

    # 1. 국가별 월별 스트리밍 변화율 분석
    country_monthly_streams = data.groupby(['Country', 'Month'])['streams'].sum().reset_index()
    country_monthly_streams['change_rate'] = country_monthly_streams.groupby('Country')['streams'].pct_change() * 100
    country_monthly_streams['change_rate'] = country_monthly_streams['change_rate'].round(2)
    country_monthly_streams_path = os.path.join(final_output_folder, "country_monthly_streams_with_rate.csv")
    country_monthly_streams.to_csv(country_monthly_streams_path, index=False, encoding='utf-8-sig')

    plt.figure(figsize=(12, 8))
    for country in country_monthly_streams['Country'].unique():
        subset = country_monthly_streams[country_monthly_streams['Country'] == country]
        plt.plot(subset['Month'].astype(str), subset['change_rate'], label=country)
    plt.title('국가별 월별 스트리밍 변화율')
    plt.xlabel('월')
    plt.ylabel('변화율 (%)')
    plt.legend()
    plt.xticks(rotation=45)
    change_rate_plot_path = os.path.join(final_output_folder, "1_change_rate_by_country.png")
    plt.savefig(change_rate_plot_path, dpi=300, bbox_inches='tight')
    plt.close()

    # 2. 국가별 스트리밍 패턴 클러스터링
    pivot_table = country_monthly_streams.pivot(index='Month', columns='Country', values='streams').fillna(0)
    kmeans = KMeans(n_clusters=3, random_state=42).fit(pivot_table.T)
    clusters = pd.DataFrame({'Country': pivot_table.columns, 'Cluster': kmeans.labels_})
    clusters_path = os.path.join(final_output_folder, "country_clusters.csv")
    clusters.to_csv(clusters_path, index=False, encoding='utf-8-sig')

    pca = PCA(n_components=2)
    reduced_data = pca.fit_transform(pivot_table.T)
    plt.figure(figsize=(10, 10))
    for cluster_id in range(kmeans.n_clusters):
        cluster_points = reduced_data[kmeans.labels_ == cluster_id]
        plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'클러스터 {cluster_id}')
    plt.title('국가 클러스터링 시각화')
    plt.xlabel('PCA 구성요소 1')
    plt.ylabel('PCA 구성요소 2')
    plt.legend()
    cluster_visualization_path = os.path.join(final_output_folder, "2_country_clustering_visualization.png")
    plt.savefig(cluster_visualization_path, dpi=300, bbox_inches='tight')
    plt.close()

    # 클러스터별 평균 스트리밍 수 시각화
    cluster_averages = pivot_table.T.groupby(kmeans.labels_).mean()
    plt.figure(figsize=(12, 6))
    for cluster_id in cluster_averages.index:
        plt.plot(cluster_averages.columns, cluster_averages.loc[cluster_id], label=f'클러스터 {cluster_id}')
    plt.title('클러스터별 평균 스트리밍 수')
    plt.xlabel('월')
    plt.ylabel('평균 스트리밍 수')
    plt.legend()
    cluster_avg_visualization_path = os.path.join(final_output_folder, "2b_cluster_avg_visualization.png")
    plt.savefig(cluster_avg_visualization_path, dpi=300, bbox_inches='tight')
    plt.close()

    # 3. 월별 상위 곡 분석
    top_tracks = data.groupby(['Month', 'track_name'])['streams'].sum().reset_index()
    top_tracks = top_tracks.sort_values(['Month', 'streams'], ascending=[True, False])
    top_tracks['rank'] = top_tracks.groupby('Month')['streams'].rank(ascending=False)
    top_tracks = top_tracks[top_tracks['rank'] <= 5]
    top_tracks_path = os.path.join(final_output_folder, "top_tracks_by_month.csv")
    top_tracks.to_csv(top_tracks_path, index=False, encoding='utf-8-sig')

    plt.figure(figsize=(14, 8))
    for month in top_tracks['Month'].unique():
        subset = top_tracks[top_tracks['Month'] == month]
        plt.bar(subset['track_name'], subset['streams'], label=str(month))
    plt.title('월별 상위 곡 스트리밍 수')
    plt.xlabel('곡 이름')
    plt.ylabel('스트리밍 수')
    plt.xticks(rotation=45, fontsize=10)
    plt.legend()
    top_tracks_plot_path = os.path.join(final_output_folder, "3_top_tracks_visualization.png")
    plt.savefig(top_tracks_plot_path, dpi=300, bbox_inches='tight')
    plt.close()

    # 인사이트 및 PDF 보고서 생성
    insights = [
        f"분석한 국가 수: {data['Country'].nunique()}",
        f"분석된 총 스트리밍 수: {data['streams'].sum():,}",
        "\n클러스터링 결과:",
    ]

    cluster_summary = clusters.groupby('Cluster')['Country'].apply(list)
    for cluster, countries in cluster_summary.items():
        insights.append(f"클러스터 {cluster}: {', '.join(countries)}")

    report_pdf_path = os.path.join(final_output_folder, "music_trends_report.pdf")
    generate_report_pdf(
        insights,
        [
            change_rate_plot_path,
            cluster_visualization_path,
            cluster_avg_visualization_path,
            top_tracks_plot_path,
        ],
        report_pdf_path,
    )

    print("PDF 보고서 저장 완료.")

if __name__ == "__main__":
    input_folder = "./spotify_data"
    intermediate_folder = "./country_data"
    final_output_folder = "./final_data"

    merge_by_country(input_folder, intermediate_folder, final_output_folder)
    analyze_music_trends(final_output_folder)
