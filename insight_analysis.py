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
    국가별 최대 및 최소 스트리밍 데이터를 시각화합니다 (그룹화된 막대 그래프).
    """
    if os.path.exists(max_stream_path) and os.path.exists(min_stream_path):
        max_df = pd.read_csv(max_stream_path)
        min_df = pd.read_csv(min_stream_path)

        max_df['Type'] = '최대'
        min_df['Type'] = '최소'
        combined_df = pd.concat([max_df, min_df], ignore_index=True)

        plt.figure(figsize=(16, 10))
        sns.barplot(data=combined_df, x='Country', y='streams', hue='Type', ci=None)
        plt.title("국가별 최대 및 최소 스트리밍 트렌드", fontsize=16)
        plt.xlabel("국가", fontsize=14)
        plt.ylabel("스트리밍 수", fontsize=14)
        plt.xticks(rotation=45)
        plt.legend(title="유형", fontsize=12)
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
    top_items = pivot_data.sum(axis=1).nlargest(20).index  # 상위 20개만 선택
    filtered_data = pivot_data.loc[top_items]
    filtered_data.columns = pd.to_datetime(filtered_data.columns)
    filtered_data = filtered_data.sort_index(axis=1)

    plt.figure(figsize=(14, 8))
    sns.heatmap(
        filtered_data,
        cmap="coolwarm",
        cbar_kws={"label": "스트리밍 수"},
        linewidths=0.5
    )
    plt.title("월별 인기 곡/아티스트 스트리밍 히트맵", fontsize=16)
    plt.xlabel("월", fontsize=12)
    plt.ylabel("곡/아티스트", fontsize=12)
    plt.xticks(rotation=45, fontsize=10)
    plt.yticks(fontsize=10)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"히트맵 저장 완료: {output_path}")

def visualize_artist_overlap(local_artists, global_artists, output_path):
    """
    로컬 및 글로벌 아티스트 비교 비율을 시각화합니다 (스택형 바 차트).
    """
    overlap_count = len(local_artists & global_artists)
    local_only_count = len(local_artists - global_artists)
    global_only_count = len(global_artists - local_artists)

    categories = ['겹치는 아티스트', '로컬 전용', '글로벌 전용']
    counts = [overlap_count, local_only_count, global_only_count]

    plt.figure(figsize=(10, 6))
    sns.barplot(x=categories, y=counts, palette=['green', 'blue', 'orange'])
    plt.title("로컬 및 글로벌 아티스트 비교", fontsize=16)
    plt.xlabel("유형", fontsize=14)
    plt.ylabel("아티스트 수", fontsize=14)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"아티스트 비교 시각화 저장 완료: {output_path}")

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
        os.path.join(insights_output_folder, "country_stream_trends.png")
    )

    # 글로벌 트렌드 히트맵
    combined_data_path = os.path.join(final_data_folder, "monthly_common_tracks_and_artists.csv")
    if os.path.exists(combined_data_path):
        combined_data = pd.read_csv(combined_data_path)
        visualize_global_trends_heatmap(
            combined_data,
            os.path.join(insights_output_folder, "global_trends_heatmap.png")
        )

    # 로컬/글로벌 아티스트 비교
    top_artists_path = os.path.join(final_data_folder, "top_artists_by_country.csv")
    global_artists_path = os.path.join(final_data_folder, "global_top_artists.csv")
    if os.path.exists(top_artists_path) and os.path.exists(global_artists_path):
        local_df = pd.read_csv(top_artists_path)
        global_df = pd.read_csv(global_artists_path)
        local_artists_set = set(local_df['artist_names'])
        global_artists_set = set(global_df['artist_names'])
        visualize_artist_overlap(
            local_artists_set,
            global_artists_set,
            os.path.join(insights_output_folder, "artist_overlap_comparison.png")
        )
