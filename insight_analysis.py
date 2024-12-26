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

def visualize_country_stream_trends(max_stream_path, output_path):
    """
    국가별 최대 스트리밍 데이터를 시각화합니다 (정렬된 막대 그래프).
    """
    if os.path.exists(max_stream_path):
        max_df = pd.read_csv(max_stream_path)
        max_df = max_df.sort_values(by='streams', ascending=False)

        # 국가 약어를 전체 이름으로 변환 (예시 데이터)
        country_names = {
            'AU': '호주', 'BR': '브라질', 'DE': '독일', 'ES': '스페인', 'FR': '프랑스',
            'GB': '영국', 'IN': '인도', 'JP': '일본', 'KR': '한국', 'MX': '멕시코',
            'SE': '스웨덴', 'TH': '태국', 'US': '미국', 'VN': '베트남', 'ZA': '남아프리카'
        }
        max_df['Country'] = max_df['Country'].map(country_names)

        plt.figure(figsize=(14, 8))
        sns.barplot(data=max_df, x='Country', y='streams', palette='Blues_d')
        plt.title("국가별 최대 스트리밍 트렌드", fontsize=16)
        plt.xlabel("국가", fontsize=14)
        plt.ylabel("스트리밍 수", fontsize=14)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        print(f"국가별 스트리밍 트렌드 시각화 저장 완료: {output_path}")
    else:
        print(f"{max_stream_path} 파일이 존재하지 않습니다.")

def visualize_global_trends_heatmap(data, output_path):
    """
    월별 인기 곡/아티스트의 스트리밍 수를 히트맵으로 시각화합니다.
    """
    pivot_data = data.pivot(index='Name', columns='Month', values='streams').fillna(0)
    top_items = pivot_data.sum(axis=1).nlargest(20).index  # 상위 20개만 선택
    filtered_data = pivot_data.loc[top_items]

    # 월만 표시 (YYYY-MM 형식)
    filtered_data.columns = pd.to_datetime(filtered_data.columns).strftime('%Y-%m')

    plt.figure(figsize=(14, 8))
    sns.heatmap(
        filtered_data,
        cmap="Blues",
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

if __name__ == "__main__":
    # 파일 경로 정의
    final_data_folder = "./final_data"
    insights_output_folder = "./insights"

    if not os.path.exists(insights_output_folder):
        os.makedirs(insights_output_folder)

    # 국가별 최대 스트리밍 시각화
    visualize_country_stream_trends(
        os.path.join(final_data_folder, "max_stream_month.csv"),
        os.path.join(insights_output_folder, "country_stream_trends_sorted.png")
    )

    # 글로벌 트렌드 히트맵
    combined_data_path = os.path.join(final_data_folder, "monthly_common_tracks_and_artists.csv")
    if os.path.exists(combined_data_path):
        combined_data = pd.read_csv(combined_data_path)
        visualize_global_trends_heatmap(
            combined_data,
            os.path.join(insights_output_folder, "global_trends_heatmap_blue.png")
        )
