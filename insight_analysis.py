import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib_venn import venn2
from matplotlib import font_manager

# 한글 폰트 설정
def setup_korean_font():
    font_path = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
    if os.path.exists(font_path):
        font_manager.fontManager.addfont(font_path)
        plt.rcParams['font.family'] = font_manager.FontProperties(fname=font_path).get_name()
        plt.rcParams['axes.unicode_minus'] = False
        print(f"한글 폰트 설정 완료: {font_path}")
    else:
        print("폰트를 찾을 수 없습니다.")

setup_korean_font()

def analyze_insights(final_data_folder, insights_folder):
    """
    인사이트를 분석하고 시각화합니다.

    Args:
        final_data_folder (str): 분석할 CSV 파일들이 있는 폴더 경로
        insights_folder (str): 인사이트 결과를 저장할 폴더 경로
    """
    if not os.path.exists(insights_folder):
        os.makedirs(insights_folder)
        print(f"결과 폴더 생성 완료: {insights_folder}")

    # 1. 국가별 월별 최대/최소 스트리밍 트렌드 분석
    max_stream_path = os.path.join(final_data_folder, "max_stream_month.csv")
    min_stream_path = os.path.join(final_data_folder, "min_stream_month.csv")

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
        plt.savefig(os.path.join(insights_folder, "country_stream_trends.png"))
        plt.close()

    # 2. 로컬 및 글로벌 인기 아티스트 비교
    top_artists_path = os.path.join(final_data_folder, "top_artists_by_country.csv")
    global_artists_path = os.path.join(final_data_folder, "global_top_artists.csv")

    if os.path.exists(top_artists_path) and os.path.exists(global_artists_path):
        local_df = pd.read_csv(top_artists_path)
        global_df = pd.read_csv(global_artists_path)

        local_artists = set(local_df['artist_names'])
        global_artists = set(global_df['artist_names'])

        plt.figure(figsize=(8, 8))
        venn2([local_artists, global_artists], ('로컬 아티스트', '글로벌 아티스트'))
        plt.title("로컬 및 글로벌 인기 아티스트 비교")
        plt.savefig(os.path.join(insights_folder, "artist_overlap.png"))
        plt.close()

    # 3. 월별 인기 곡 및 아티스트 트렌드 분석
    combined_data_path = os.path.join(final_data_folder, "monthly_common_tracks_and_artists.csv")

    if os.path.exists(combined_data_path):
        combined_df = pd.read_csv(combined_data_path)

        plt.figure(figsize=(12, 8))
        for name, group in combined_df.groupby('Name'):
            plt.plot(group['Month'], group['streams'], label=name)

        plt.title("월별 인기 곡 및 아티스트 트렌드")
        plt.xlabel("월")
        plt.ylabel("스트리밍 수")
        plt.legend()
        plt.savefig(os.path.join(insights_folder, "global_trends.png"))
        plt.close()

    print("인사이트 분석 및 저장 완료.")

if __name__ == "__main__":
    final_data_folder = "./final_data"
    insights_output_folder = "./insights"
    analyze_insights(final_data_folder, insights_output_folder)
