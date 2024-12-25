import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib_venn import venn2
from matplotlib import font_manager

def setup_korean_font():
    """
    한글 폰트를 설정하는 함수.
    """
    font_path = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
    if os.path.exists(font_path):
        font_manager.fontManager.addfont(font_path)
        plt.rcParams['font.family'] = font_manager.FontProperties(fname=font_path).get_name()
        plt.rcParams['axes.unicode_minus'] = False
        print(f"한글 폰트 설정 완료: {font_path}")
    else:
        print("폰트를 찾을 수 없습니다. Actions 설정을 확인하세요.")

setup_korean_font()

def analyze_and_visualize_insights(output_folder):
    """
    데이터를 분석하고 시각화하여 결과를 저장하는 함수.

    Args:
        output_folder (str): 결과가 저장될 폴더 경로.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"결과 폴더 생성 완료: {output_folder}")

    print(f"데이터 분석 및 시각화 결과는 {output_folder}에 저장됩니다.")

    # 생성된 파일 리스트
    generated_files = []

    # 1. 국가별 월별 스트리밍 최대/최소 트렌드 분석
    max_stream_path = os.path.join(output_folder, "max_stream_month.csv")
    min_stream_path = os.path.join(output_folder, "min_stream_month.csv")

    if os.path.exists(max_stream_path) and os.path.exists(min_stream_path):
        max_df = pd.read_csv(max_stream_path)
        min_df = pd.read_csv(min_stream_path)
        plt.figure(figsize=(14, 8))
        plt.bar(max_df['Country'], max_df['streams'] / 1e6, label='최대 스트리밍 (백만 단위)', color='blue', alpha=0.6)
        plt.bar(min_df['Country'], min_df['streams'] / 1e6, label='최소 스트리밍 (백만 단위)', color='red', alpha=0.6)
        plt.title("국가별 최대 및 최소 스트리밍 트렌드", fontsize=16)
        plt.xlabel("국가", fontsize=14)
        plt.ylabel("스트리밍 수 (백만)", fontsize=14)
        plt.xticks(rotation=45, fontsize=12)
        plt.legend(fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        result_path = os.path.join(output_folder, "country_stream_trends_improved.png")
        plt.savefig(result_path)
        generated_files.append(result_path)
        print(f"시각화 결과 저장: {result_path}")
        plt.close()
    else:
        print("max_stream_month.csv 또는 min_stream_month.csv 파일이 존재하지 않습니다.")

    # 2. 로컬 및 글로벌 인기 아티스트 비교
    top_artists_path = os.path.join(output_folder, "top_artists_by_country.csv")
    global_artists_path = os.path.join(output_folder, "global_top_artists.csv")

    if os.path.exists(top_artists_path) and os.path.exists(global_artists_path):
        local_df = pd.read_csv(top_artists_path)
        global_df = pd.read_csv(global_artists_path)
        local_artists = set(local_df['artist_names'])
        global_artists = set(global_df['artist_names'])
        plt.figure(figsize=(12, 8))
        venn = venn2([local_artists, global_artists], ('로컬 아티스트', '글로벌 아티스트'))
        plt.title("로컬 및 글로벌 인기 아티스트 비교", fontsize=18, fontweight='bold')

        # 교집합 강조
        for text in venn.subset_labels:
            if text:
                text.set_fontsize(14)
                text.set_color('black')

        # 라벨의 폰트 크기와 색상 조정
        for text in venn.set_labels:
            if text:
                text.set_fontsize(16)
                text.set_fontweight('bold')

        # 각 집합의 크기 설명 추가
        plt.text(-0.8, -0.6, f"로컬 아티스트 수: {len(local_artists)}", fontsize=12, color='blue')
        plt.text(0.6, -0.6, f"글로벌 아티스트 수: {len(global_artists)}", fontsize=12, color='orange')

        plt.tight_layout()
        result_path = os.path.join(output_folder, "artist_overlap_improved.png")
        plt.savefig(result_path)
        generated_files.append(result_path)
        print(f"시각화 결과 저장: {result_path}")
        plt.close()
    else:
        print("top_artists_by_country.csv 또는 global_top_artists.csv 파일이 존재하지 않습니다.")

    # 3. 월별 인기 곡 및 아티스트 트렌드 분석
    combined_data_path = os.path.join(output_folder, "monthly_common_tracks_and_artists.csv")
    if os.path.exists(combined_data_path):
        combined_df = pd.read_csv(combined_data_path)
        top_items = combined_df.groupby('Name')['streams'].sum().nlargest(5).index  # 상위 5개 곡만 표시
        filtered_df = combined_df[combined_df['Name'].isin(top_items)]

        plt.figure(figsize=(14, 8))
        colors = plt.cm.get_cmap('tab10', len(top_items))
        for i, (name, group) in enumerate(filtered_df.groupby('Name')):
            plt.plot(group['Month'], group['streams'] / 1e6, label=name, marker='o', linewidth=2, color=colors(i))
            # 상승/하강 트렌드 강조
            for idx, row in group.iterrows():
                plt.text(row['Month'], row['streams'] / 1e6, f"{int(row['streams'] / 1e6)}M", fontsize=10)

        plt.title("월별 인기 곡 트렌드 (상위 5곡)", fontsize=18, fontweight='bold')
        plt.xlabel("월", fontsize=14)
        plt.ylabel("스트리밍 수 (백만)", fontsize=14)
        plt.xticks(rotation=45, fontsize=12)
        plt.legend(fontsize=12, title="곡 이름", loc='upper left')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        result_path = os.path.join(output_folder, "global_trends_improved.png")
        plt.savefig(result_path)
        generated_files.append(result_path)
        print(f"시각화 결과 저장: {result_path}")
        plt.close()
    else:
        print("monthly_common_tracks_and_artists.csv 파일이 존재하지 않습니다.")

    # 생성된 파일 리스트 출력
    print("생성된 파일들:", generated_files)

if __name__ == "__main__":
    final_data_folder = "./final_data"
    insights_output_folder = "./insights"
    analyze_and_visualize_insights(insights_output_folder)
