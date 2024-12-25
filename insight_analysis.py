import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib_venn import venn2
from matplotlib import font_manager

def setup_korean_font():
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
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"결과 폴더 생성 완료: {output_folder}")

    print(f"데이터 분석 및 시각화 결과는 {output_folder}에 저장됩니다.")

    # 1. 로컬 및 글로벌 인기 아티스트 비교 (artist_overlap_improved.png)
    top_artists_path = os.path.join("final_data", "top_artists_by_country.csv")
    global_artists_path = os.path.join("final_data", "global_top_artists.csv")

    if os.path.exists(top_artists_path) and os.path.exists(global_artists_path):
        local_df = pd.read_csv(top_artists_path)
        global_df = pd.read_csv(global_artists_path)
        local_artists = set(local_df['artist_names'])
        global_artists = set(global_df['artist_names'])

        # Venn Diagram
        plt.figure(figsize=(10, 10))
        venn = venn2([local_artists, global_artists], ('로컬 아티스트', '글로벌 아티스트'))
        plt.title("로컬 및 글로벌 인기 아티스트 비교", fontsize=16)

        # Add detailed artist names
        local_only = local_artists - global_artists
        global_only = global_artists - local_artists
        overlap = local_artists & global_artists

        plt.text(-0.7, -0.5, f"로컬 전용:\n" + "\n".join(list(local_only)[:5]) + ("..." if len(local_only) > 5 else ""), fontsize=10)
        plt.text(0.7, -0.5, f"글로벌 전용:\n" + "\n".join(list(global_only)[:5]) + ("..." if len(global_only) > 5 else ""), fontsize=10)
        plt.text(0, 0, f"겹치는 아티스트 수: {len(overlap)}", fontsize=12, ha='center')

        plt.tight_layout()
        result_path = os.path.join(output_folder, "artist_overlap_improved.png")
        plt.savefig(result_path)
        print(f"시각화 결과 저장: {result_path}")
        plt.close()

    # 2. 월별 인기 곡 및 아티스트 트렌드 (global_trends_improved.png)
    combined_data_path = os.path.join("final_data", "monthly_common_tracks_and_artists.csv")
    if os.path.exists(combined_data_path):
        combined_df = pd.read_csv(combined_data_path)
        top_items = combined_df.groupby('Name')['streams'].sum().nlargest(5).index  # 상위 5개 항목만 표시
        filtered_df = combined_df[combined_df['Name'].isin(top_items)]

        plt.figure(figsize=(14, 8))
        for name, group in filtered_df.groupby('Name'):
            plt.plot(group['Month'], group['streams'] / 1e6, label=name, marker='o')
            for x, y in zip(group['Month'], group['streams'] / 1e6):
                plt.text(x, y, f"{y:.1f}M", fontsize=8, ha='center')  # 각 데이터 포인트에 라벨 추가

        plt.title("월별 인기 곡 및 아티스트 트렌드", fontsize=16)
        plt.xlabel("월", fontsize=14)
        plt.ylabel("스트리밍 수 (백만)", fontsize=14)
        plt.xticks(rotation=45, fontsize=12)
        plt.legend(fontsize=10, loc='upper left')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        result_path = os.path.join(output_folder, "global_trends_improved.png")
        plt.savefig(result_path)
        print(f"시각화 결과 저장: {result_path}")
        plt.close()

if __name__ == "__main__":
    insights_output_folder = "./insights"
    analyze_and_visualize_insights(insights_output_folder)
