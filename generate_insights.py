import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager

# 한글 폰트 설정 함수
def setup_korean_font():
    """
    한글 폰트를 설정하는 함수입니다. 시스템 환경에 설치된 폰트를 기반으로 설정합니다.
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

# 데이터 시각화 및 인사이트 도출 함수
def analyze_and_visualize_insights(output_folder):
    """
    주어진 데이터를 기반으로 인사이트를 도출하고 시각화합니다.

    Args:
        output_folder (str): 분석 결과 데이터가 저장된 폴더 경로

    결과물:
        1. 국가별 월별 스트리밍 최대/최소 트렌드 분석
        2. 로컬 및 글로벌 인기 아티스트 비교
        3. 월별 인기 곡 및 아티스트 트렌드 분석
    """

    # 1. 국가별 월별 스트리밍 최대/최소 트렌드 분석
    max_stream_path = os.path.join(output_folder, "max_stream_month.csv")
    min_stream_path = os.path.join(output_folder, "min_stream_month.csv")

    if os.path.exists(max_stream_path) and os.path.exists(min_stream_path):
        max_df = pd.read_csv(max_stream_path)
        min_df = pd.read_csv(min_stream_path)

        plt.figure(figsize=(12, 6))
        plt.bar(max_df['Country'], max_df['streams'], label='최대 스트리밍', color='blue', alpha=0.6)
        plt.bar(min_df['Country'], min_df['streams'], label='최소 스트리밍', color='red', alpha=0.6)
        plt.title("국가별 최대 및 최소 스트리밍 트렌드")
        plt.xlabel("국가")
        plt.ylabel("스트리밍 수")
        plt.legend()
        plt.savefig(os.path.join(output_folder, "country_stream_trends.png"))
        plt.close()

    # 2. 로컬 및 글로벌 인기 아티스트 비교
    top_artists_path = os.path.join(output_folder, "top_artists_by_country.csv")
    global_artists_path = os.path.join(output_folder, "global_top_artists.csv")

    if os.path.exists(top_artists_path) and os.path.exists(global_artists_path):
        local_df = pd.read_csv(top_artists_path)
        global_df = pd.read_csv(global_artists_path)

        local_artists = set(local_df['artist_names'])
        global_artists = set(global_df['artist_names'])

        # Venn Diagram
        from matplotlib_venn import venn2
        plt.figure(figsize=(8, 8))
        venn = venn2([local_artists, global_artists], ('로컬 아티스트', '글로벌 아티스트'))
        plt.title("로컬 및 글로벌 인기 아티스트 비교")
        plt.savefig(os.path.join(output_folder, "artist_overlap.png"))
        plt.close()

    # 3. 월별 인기 곡 및 아티스트 트렌드 분석
    combined_data_path = os.path.join(output_folder, "monthly_common_tracks_and_artists.csv")

    if os.path.exists(combined_data_path):
        combined_df = pd.read_csv(combined_data_path)

        plt.figure(figsize=(12, 8))
        for name, group in combined_df.groupby('Name'):
            plt.plot(group['Month'], group['streams'], label=name)

        plt.title("월별 인기 곡 및 아티스트 트렌드")
        plt.xlabel("월")
        plt.ylabel("스트리밍 수")
        plt.legend()
        plt.savefig(os.path.join(output_folder, "global_trends.png"))
        plt.close()

    print("분석 및 시각화 완료. 결과물은 해당 폴더에 저장되었습니다.")

if __name__ == "__main__":
    output_folder = "./final_data"
    analyze_and_visualize_insights(output_folder)
