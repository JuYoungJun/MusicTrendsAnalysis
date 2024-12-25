import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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


def plot_country_stream_trends(max_stream_data, min_stream_data, output_folder):
    max_stream_data = max_stream_data.nlargest(10, 'streams')
    min_stream_data = min_stream_data[min_stream_data['Country'].isin(max_stream_data['Country'])]

    countries = max_stream_data['Country'].tolist()
    max_stream_data = max_stream_data.set_index('Country').reindex(countries)
    min_stream_data = min_stream_data.set_index('Country').reindex(countries)

    plt.figure(figsize=(12, 8))
    bar_width = 0.35
    indices = range(len(countries))
    plt.bar(indices, max_stream_data['streams'] / 1e6, width=bar_width, label='최대 스트리밍 (백만 단위)', color='blue', alpha=0.6)
    plt.bar([i + bar_width for i in indices], min_stream_data['streams'] / 1e6, width=bar_width, label='최소 스트리밍 (백만 단위)', color='red', alpha=0.6)

    plt.xticks([i + bar_width / 2 for i in indices], countries, rotation=45, fontsize=12)
    plt.xlabel("국가", fontsize=14)
    plt.ylabel("스트리밍 수 (백만)", fontsize=14)
    plt.title("국가별 최대 및 최소 스트리밍 비교 (상위 10개 국가)", fontsize=16)
    plt.legend(fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    output_path = os.path.join(output_folder, "country_stream_trends_improved.png")
    plt.savefig(output_path)
    print(f"국가별 스트리밍 비교 그래프 저장 완료: {output_path}")
    plt.close()


def plot_global_trends_heatmap(data, output_folder):
    top_items = data.groupby('Name')['streams'].sum().nlargest(10).index
    filtered_df = data[data['Name'].isin(top_items)]
    pivot_df = filtered_df.pivot(index='Name', columns='Month', values='streams')

    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_df, annot=True, fmt=".0f", cmap="Blues", cbar_kws={'label': '스트리밍 수 (백만)'})
    plt.title("월별 인기 곡 및 아티스트 트렌드 (히트맵)", fontsize=16)
    plt.xlabel("월", fontsize=12)
    plt.ylabel("곡/아티스트", fontsize=12)
    plt.tight_layout()
    output_path = os.path.join(output_folder, "global_trends_heatmap.png")
    plt.savefig(output_path)
    print(f"히트맵 저장 완료: {output_path}")
    plt.close()


def plot_artist_overlap(data_local, data_global, output_folder):
    local_artists = set(data_local['artist_names'])
    global_artists = set(data_global['artist_names'])

    common_artists = local_artists.intersection(global_artists)
    only_local = local_artists - global_artists
    only_global = global_artists - local_artists

    plt.figure(figsize=(12, 6))
    categories = ['로컬 전용', '글로벌 전용', '공통 아티스트']
    counts = [len(only_local), len(only_global), len(common_artists)]
    plt.bar(categories, counts, color=['blue', 'red', 'green'])
    plt.title("로컬 및 글로벌 인기 아티스트 비교", fontsize=16)
    plt.ylabel("아티스트 수", fontsize=12)
    plt.xlabel("카테고리", fontsize=12)
    plt.tight_layout()
    output_path = os.path.join(output_folder, "artist_overlap_comparison.png")
    plt.savefig(output_path)
    print(f"아티스트 비교 막대 그래프 저장 완료: {output_path}")
    plt.close()

    with open(os.path.join(output_folder, "common_artists.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(common_artists)))
        print(f"공통 아티스트 목록 저장 완료: common_artists.txt")


def analyze_and_visualize_insights(output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"결과 폴더 생성 완료: {output_folder}")

    max_stream_path = os.path.join("./final_data", "max_stream_month.csv")
    min_stream_path = os.path.join("./final_data", "min_stream_month.csv")
    global_artists_path = os.path.join("./final_data", "global_top_artists.csv")
    local_artists_path = os.path.join("./final_data", "top_artists_by_country.csv")
    trends_path = os.path.join("./final_data", "monthly_common_tracks_and_artists.csv")

    if os.path.exists(max_stream_path) and os.path.exists(min_stream_path):
        max_stream_data = pd.read_csv(max_stream_path)
        min_stream_data = pd.read_csv(min_stream_path)
        plot_country_stream_trends(max_stream_data, min_stream_data, output_folder)
    else:
        print("max_stream_month.csv 또는 min_stream_month.csv 파일이 존재하지 않습니다.")

    if os.path.exists(global_artists_path) and os.path.exists(local_artists_path):
        global_artists = pd.read_csv(global_artists_path)
        local_artists = pd.read_csv(local_artists_path)
        plot_artist_overlap(local_artists, global_artists, output_folder)
    else:
        print("top_artists_by_country.csv 또는 global_top_artists.csv 파일이 존재하지 않습니다.")

    if os.path.exists(trends_path):
        trends_data = pd.read_csv(trends_path)
        plot_global_trends_heatmap(trends_data, output_folder)
    else:
        print("monthly_common_tracks_and_artists.csv 파일이 존재하지 않습니다.")


if __name__ == "__main__":
    insights_output_folder = "./insights"
    analyze_and_visualize_insights(insights_output_folder)
