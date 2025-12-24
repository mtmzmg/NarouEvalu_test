import streamlit as st
import duckdb
import pandas as pd
import math

st.set_page_config(page_title="なろう小説 査読ツール", layout="wide")

PARQUET_PATH = "data/*.parquet"

# ==========================================
# 💾 1. 軽量インデックス（検索用データ）の読み込み
# ==========================================
# ここでは「あらすじ(story)」は読み込みません！
# 読み込むとメモリが死ぬため、IDと検索用項目だけを保持します。
@st.cache_data(ttl=3600)
def load_index_data():
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL parquet; LOAD parquet;")
    
    # 必要最小限の列だけを取得
    query = f"""
        SELECT ncode, title, genre, global_point, length, general_lastup, keyword
        FROM '{PARQUET_PATH}'
    """
    df = con.execute(query).fetchdf()
    con.close()
    
    # 前処理
    df["global_point"] = df["global_point"].fillna(0).astype(int)
    df["genre"] = df["genre"].fillna("不明")
    return df

# アプリ起動時にインデックスだけメモリに乗せる（約30MB〜50MB程度で軽量）
try:
    with st.spinner("検索インデックスを構築中..."):
        df_index = load_index_data()
except Exception as e:
    st.error(f"データ読み込みエラー: {e}")
    st.stop()

# ==========================================
# 🔍 検索条件 & フィルタリング
# ==========================================
st.sidebar.header("検索条件")

genres = ["すべて"] + sorted(df_index["genre"].unique().tolist())
genre_filter = st.sidebar.selectbox("ジャンル", genres)
keyword = st.sidebar.text_input("タイトル・キーワード検索")
min_point = st.sidebar.number_input("最低総合ポイント", value=0, step=1000)

# フィルタリング実行（Pandas上で高速処理）
df_view = df_index

if min_point > 0:
    df_view = df_view[df_view["global_point"] >= min_point]

if genre_filter != "すべて":
    df_view = df_view[df_view["genre"] == genre_filter]

if keyword:
    # タイトル または キーワード で検索
    # keywordカラムがある場合は結合して検索すると便利
    mask = df_view["title"].str.contains(keyword, na=False) | \
           df_view["keyword"].str.contains(keyword, na=False)
    df_view = df_view[mask]

# 並び替え
df_view = df_view.sort_values("global_point", ascending=False)

# ==========================================
# 📑 ページネーション & あらすじの結合
# ==========================================
st.title(f"📚 なろう小説分析 ({len(df_view):,}件)")

PAGE_SIZE = 50  # あらすじを表示するなら1ページ50件くらいが見やすい
total_rows = len(df_view)
total_pages = math.ceil(total_rows / PAGE_SIZE) if total_rows > 0 else 1

col1, col2 = st.columns([2, 8])
with col1:
    current_page = st.number_input("ページ", min_value=1, max_value=total_pages, value=1)

# 1. まず、表示すべき「Nコード」のリストを決定する
start_idx = (current_page - 1) * PAGE_SIZE
end_idx = start_idx + PAGE_SIZE
df_display_index = df_view.iloc[start_idx:end_idx].copy()

# 2. そのNコードに対応する「あらすじ」だけをParquetから取ってくる（ここがミソ！）
if not df_display_index.empty:
    target_ncodes = df_display_index["ncode"].tolist()
    
    # DuckDBで「このNコードたちのあらすじをくれ」と問い合わせる
    # IN句用のプレースホルダーを作る (?, ?, ?)
    placeholders = ', '.join(['?'] * len(target_ncodes))
    
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL parquet; LOAD parquet;")
    
    story_query = f"""
        SELECT ncode, story 
        FROM '{PARQUET_PATH}' 
        WHERE ncode IN ({placeholders})
    """
    
    # 実行してあらすじを取得（50件分だけなので一瞬）
    df_stories = con.execute(story_query, target_ncodes).fetchdf()
    con.close()
    
    # 3. 検索結果とあらすじを合体させる
    df_final = pd.merge(df_display_index, df_stories, on="ncode", how="left")
else:
    df_final = pd.DataFrame()

# ==========================================
# 📝 画面表示
# ==========================================
if not df_final.empty:
    # データフレーム表示（あらすじを含む）
    st.data_editor(
        df_final,
        column_config={
            "ncode": "Nコード",
            "title": st.column_config.TextColumn("タイトル", width="medium"),
            "story": st.column_config.TextColumn("あらすじ", width="large"), # 幅広で表示
            "genre": "ジャンル",
            "global_point": "総合Pt",
            "length": "文字数",
            "general_lastup": "最終更新",
            "keyword": st.column_config.TextColumn("キーワード", width="small"),
        },
        hide_index=True,
        use_container_width=True,
        height=1000 # 縦長にして見やすく
    )
else:
    st.warning("条件に一致する作品が見つかりませんでした。")
