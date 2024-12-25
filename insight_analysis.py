import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def setup_korean_font():
    """
    한글 폰트를 설정하는 함수입니다.
    """
    font_path = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
    if os.path.exists(font_path):
        from matplotlib import font_manager
        font_manager.fontManager.addfont(font_path)
        plt.rcParams['font.family'] = font_manager.FontProperties(fname=font_path).get_name()
        plt.rcParams['axes.unicode_minus'] = False
        print(f"한글 폰트 설정 완료: {font_path}")
    else:
        print("폰트를 찾을 수 없습니다. Actions 설정을 확인하세요.")


setup_korean_font()


def visualize_country_stream_trends(max_stream_path, min_stream_path, output_path):
    """
    국가별 최대 및 최소 스트리밍 데이터를 시각화합니다.
    """
    if os.path.exists(max_stream_path) and os.path.exists(min_stream_path):
        max_df = pd.read_csv(max_stream_path)
        min_df = pd.read_csv(min_stream_path)

        plt.figure(figsize=(14, 8))
        plt.bar(max_df['Country'], max_df['streams'] / 1e6, label='최대 스트리밍 (백만)', color='blue', alpha=0.6)
        plt.bar(min_df['Country'], min_df['streams'] / 1e6, label='최소 스트리밍 (백만)', color='red', alpha=0.6)
        plt.title("국가별 최대 및 최소 스트리밍 트렌드", fontsize=16)
        plt.xlabel("국가", fontsize=14)
        plt.ylabel("스트리밍 수 (백만)", fontsize=14)
        plt.xticks(rotation=45, fontsize=12)
        plt.legend(fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        print(f"국가별 스트리밍 트렌드 시각화 저장 완료: {output_path}")
    else:
        print(f"{max_stream_path} 또는 {min_stream_path} 파일이 존재하지 않습니다.")


def visualize_global_trends_heatmap(data, output_path):
    """
    월별 인기 곡/아티스트의 스트리밍 수를 히트맵으로 시각화합니다.
    """
    pivot_data = data.pivot(index='Name', columns='Month', values='streams').fillna(0)
    top_items = pivot_data.sum(axis=1).nlargest(10).index
    filtered_data = pivot_data.loc[top_items]

    plt.figure(figsize=(14, 10))
    sns.heatmap(
        filtered_data,
        annot=True,
        fmt=".0f",
        cmap="YlGnBu",
        cbar_kws={"label": "스트리밍 수 (백만)"},
        linewidths=0.5
    )
    plt.title("월별 인기 곡/아티스트 스트리밍 히트맵", fontsize=16)
    plt.xlabel("월", fontsize=14)
    plt.ylabel("곡/아티스트", fontsize=14)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"히트맵 저장 완료: {output_path}")


def visualize_artist_overlap_comparison(local_artists, global_artists, output_path):
    """
    로컬 및 글로벌 아티스트의 비교를 클러스터 방식으로 시각화합니다.
    """
    overlap_artists = list(local_artists & global_artists)
    local_only = list(local_artists - global_artists)
    global_only = list(global_artists - local_artists)

    artist_data = {
        "Artist": overlap_artists + local_only + global_only,
        "Group": ["겹치는 아티스트"] * len(overlap_artists) +
                 ["로컬 전용"] * len(local_only) +
                 ["글로벌 전용"] * len(global_only)
    }

    artist_df = pd.DataFrame(artist_data)

    # 클러스터 시각화
    plt.figure(figsize=(14, 8))
    sns.scatterplot(
        data=artist_df,
        x="Artist",
        y="Group",
        hue="Group",
        palette={"겹치는 아티스트": "green", "로컬 전용": "blue", "글로벌 전용": "orange"},
        s=100
    )

    plt.title("로컬 및 글로벌 아티스트 클러스터 비교", fontsize=16)
    plt.xlabel("아티스트", fontsize=14)
    plt.ylabel("그룹", fontsize=14)
    plt.xticks(rotation=90, fontsize=10)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"아티스트 비교 클러스터 시각화 저장 완료: {output_path}")


if __name__ == "__main__":
    # 파일 경로 정의
    final_data_folder = "./final_data"
    insights_output_folder = "./insights"

    if not os.path.exists(insights_output_folder):
        os.makedirs(insights_output_folder)

    # 국가별 최대/최소 스트리밍 시각화
    visualize_country_stream_trends(
        os.path.join(final_data_folder, "max_stream_month.csv"),
        os.path.join(final_data_folder, "min_stream_month.csv"),
        os.path.join(insights_output_folder, "country_stream_trends_improved.png")
    )

    # 글로벌 트렌드 히트맵
    combined_data_path = os.path.join(final_data_folder, "monthly_common_tracks_and_artists.csv")
    if os.path.exists(combined_data_path):
        combined_data = pd.read_csv(combined_data_path)
        visualize_global_trends_heatmap(
            combined_data,
            os.path.join(insights_output_folder, "global_trends_heatmap_improved.png")
        )

    # 로컬/글로벌 아티스트 비교 클러스터
    top_artists_path = os.path.join(final_data_folder, "top_artists_by_country.csv")
    global_artists_path = os.path.join(final_data_folder, "global_top_artists.csv")
    if os.path.exists(top_artists_path) and os.path.exists(global_artists_path):
        local_df = pd.read_csv(top_artists_path)
        global_df = pd.read_csv(global_artists_path)
        local_artists_set = set(local_df['artist_names'])
        global_artists_set = set(global_df['artist_names'])
        visualize_artist_overlap_comparison(
            local_artists_set,
            global_artists_set,
            os.path.join(insights_output_folder, "artist_overlap_comparison_improved.png")
        )
