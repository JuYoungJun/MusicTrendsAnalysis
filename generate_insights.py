import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib_venn import venn2
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

def generate_insights(output_folder):
    """
    데이터를 분석하고 유의미한 인사이트를 도출합니다.
    Args:
        output_folder (str): 분석 결과 데이터가 저장된 폴더 경로
    결과물:
        1. 국가별 스트리밍 트렌드 (최대/최소) 분석
        2. 인기 곡 및 아티스트의 스트리밍 트렌드 분석
        3. 로컬 및 글로벌 아티스트 겹침 분석
        4. 월별 트렌드 비교 시각화
    """
    # 폴더 생성
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"결과 폴더 생성 완료: {output_folder}")

    # 1. 국가별 스트리밍 최대/최소 트렌드 분석
    max_stream_path = os.path.join(output_folder, "max_stream_month.csv")
    min_stream_path = os.path.join(output_folder, "min_stream_month.csv")

    if os.path.exists(max_stream_path) and os.path.exists(min_stream_path):
        max_df = pd.read_csv(max_stream_path)
        min_df = pd.read_csv(min_stream_path)

        plt.figure(figsize=(12, 6))
        plt.bar(max_df['Country'], max_df['streams'], label='최대 스트리밍', alpha=0.7, color='blue')
        plt.bar(min_df['Country'], min_df['streams'], label='최소 스트리밍', alpha=0.7, color='orange')
        plt.title("국가별 최대 및 최소 스트리밍 트렌드")
        plt.xlabel("국가")
        plt.ylabel("스트리밍 수")
        plt.legend()
        plt.savefig(os.path.join(output_folder, "country_stream_trends.png"))
        plt.close()

    # 2. 인기 아티스트 분석 (로컬 vs 글로벌)
    local_path = os.path.join(output_folder, "top_artists_by_country.csv")
    global_path = os.path.join(output_folder, "global_top_artists.csv")

    if os.path.exists(local_path) and os.path.exists(global_path):
        local_df = pd.read_csv(local_path)
        global_df = pd.read_csv(global_path)

        local_artists = set(local_df['artist_names'])
        global_artists = set(global_df['artist_names'])

        plt.figure(figsize=(8, 8))
        venn = venn2([local_artists, global_artists], ('로컬 인기 아티스트', '글로벌 인기 아티스트'))
        plt.title("로컬과 글로벌 인기 아티스트 비교")
        plt.savefig(os.path.join(output_folder, "artist_comparison.png"))
        plt.close()

    # 3. 월별 인기 곡/아티스트 트렌드 분석
    combined_path = os.path.join(output_folder, "monthly_common_tracks_and_artists.csv")

    if os.path.exists(combined_path):
        combined_df = pd.read_csv(combined_path)

        top_tracks = combined_df.groupby('Name')['streams'].sum().nlargest(5).index
        top_data = combined_df[combined_df['Name'].isin(top_tracks)]

        plt.figure(figsize=(12, 8))
        for name, group in top_data.groupby('Name'):
            plt.plot(group['Month'], group['streams'], label=name)
        plt.title("월별 인기 곡/아티스트 트렌드")
        plt.xlabel("월")
        plt.ylabel("스트리밍 수")
        plt.legend()
        plt.savefig(os.path.join(output_folder, "monthly_trends.png"))
        plt.close()

    # 4. 전세계 스트리밍 상위 곡/아티스트 비교
    track_path = os.path.join(output_folder, "monthly_common_tracks.csv")
    artist_path = os.path.join(output_folder, "monthly_common_artists.csv")

    if os.path.exists(track_path) and os.path.exists(artist_path):
        track_df = pd.read_csv(track_path)
        artist_df = pd.read_csv(artist_path)

        track_top = track_df.groupby('track_name')['streams'].sum().nlargest(5).reset_index()
        artist_top = artist_df.groupby('artist_names')['streams'].sum().nlargest(5).reset_index()

        fig, axes = plt.subplots(1, 2, figsize=(16, 8))
        axes[0].barh(track_top['track_name'], track_top['streams'], color='purple')
        axes[0].set_title("상위 5곡 스트리밍 수")
        axes[0].invert_yaxis()

        axes[1].barh(artist_top['artist_names'], artist_top['streams'], color='green')
        axes[1].set_title("상위 5 아티스트 스트리밍 수")
        axes[1].invert_yaxis()

        plt.tight_layout()
        plt.savefig(os.path.join(output_folder, "top_tracks_artists.png"))
        plt.close()

    print("모든 분석 및 시각화 완료.")

if __name__ == "__main__":
    output_folder = "./insights"
    generate_insights(output_folder)
