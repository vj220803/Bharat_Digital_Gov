import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
from datetime import datetime
import re

# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------
st.set_page_config(
    page_title="Project Samarth",
    page_icon="🌾",
    layout="wide"
)

# ---------------------------------------------------------
# CSS
# ---------------------------------------------------------
st.markdown("""
<style>
.header {
    font-size: 45px;
    font-weight: 800;
    text-align: center;
    color: #2ecc71;
    margin-bottom: -10px;
}
.sub {
    text-align: center;
    font-size: 18px;
    color: #555;
    margin-bottom: 25px;
}
.box {
    padding: 18px;
    border-radius: 10px;
    background: #f4fff6;
    border-left: 5px solid #2ecc71;
    margin-bottom: 12px;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------
# CACHING — Major Speed Improvement
# ---------------------------------------------------------
@st.cache_resource
def get_connection():
    return duckdb.connect(database=":memory:")

@st.cache_data
def load_data_fast():
    df_crop = pd.read_parquet("crop_production.parquet")
    df_rain = pd.read_parquet("imd_rainfall.parquet")
    return df_crop, df_rain

@st.cache_resource
def setup_db(df_crop, df_rain):
    con = get_connection()
    con.register("crop", df_crop)
    con.register("rain", df_rain)
    return con


# ---------------------------------------------------------
# NATURAL LANGUAGE QUERY PLANNER  (Optimized)
# ---------------------------------------------------------
def samarth_query(q):
    q = q.lower()

    # Top N crops in a state
    m = re.search(r"top\s+(\d+)\s+crops?\s+in\s+(.+)", q)
    if m:
        n, state = m.groups()
        return f"""
        SELECT crop, SUM(production_mt) AS total_prod
        FROM crop
        WHERE LOWER(state) LIKE '%{state}%'
        GROUP BY crop
        ORDER BY total_prod DESC
        LIMIT {int(n)};
        """, f"Top {n} crops in {state.title()}"

    # Crop production in state
    m = re.search(r"production of (.+) in (.+)", q)
    if m:
        crop, state = m.groups()
        return f"""
        SELECT district, SUM(production_mt) AS total_prod
        FROM crop
        WHERE LOWER(crop) LIKE '%{crop}%' 
        AND LOWER(state) LIKE '%{state}%'
        GROUP BY district
        ORDER BY total_prod DESC;
        """, f"Production of {crop.title()} in {state.title()}"

    # Rainfall state
    m = re.search(r"rainfall in (.+)", q)
    if m:
        state = m.group(1)
        return f"""
        SELECT year, AVG(annual) AS avg_rainfall
        FROM rain
        WHERE LOWER(state) LIKE '%{state}%'
        GROUP BY year
        ORDER BY year;
        """, f"Rainfall Trend in {state.title()}"

    return None, None


# ---------------------------------------------------------
# VISUALIZATION GENERATOR
# ---------------------------------------------------------
def create_fig(df, title):
    if "crop" in df.columns:
        return px.bar(df, x="crop", y="total_prod", title=title, color="total_prod")

    if "district" in df.columns:
        return px.bar(df, x="district", y="total_prod", title=title)

    if "year" in df.columns:
        return px.line(df, x="year", y="avg_rainfall", markers=True, title=title)

    return None


# ---------------------------------------------------------
# MAIN UI
# ---------------------------------------------------------
def main():

    st.markdown('<div class="header">🌾 Project Samarth</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub">Fast • Intelligent • Agricultural Insights</div>', unsafe_allow_html=True)

    df_crop, df_rain = load_data_fast()
    con = setup_db(df_crop, df_rain)

    tab1, tab2, tab3 = st.tabs(["🔍 Query System", "📊 Insights", "📂 Raw Data"])

    # ---------------------------------------------------------
    # TAB 1 — Q&A
    # ---------------------------------------------------------
    with tab1:
        st.markdown("#### Ask your question")
        q = st.text_input("Example: Top 5 crops in Punjab")

        if st.button("Search", type="primary"):
            sql, title = samarth_query(q)

            if sql is None:
                st.error("❌ Could not understand query. Try again.")
            else:
                df = con.execute(sql).df()
                st.success(f"✅ Found {len(df)} results")

                st.dataframe(df, use_container_width=True)
                fig = create_fig(df, title)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)

    # ---------------------------------------------------------
    # TAB 2 — Analytics
    # ---------------------------------------------------------
    with tab2:

        st.markdown("### 📌 Key Metrics")
        col1, col2, col3, col4 = st.columns(4)

        col1.metric("Total Production", f"{df_crop['production_mt'].sum():,.0f} MT")
        col2.metric("Total Crops", df_crop["crop"].nunique())
        col3.metric("States", df_crop["state"].nunique())
        col4.metric("Rainfall Entries", df_rain.shape[0])

        st.markdown("### 🌾 Top 10 Crops")
        top10 = df_crop.groupby("crop")["production_mt"].sum().nlargest(10).reset_index()
        st.bar_chart(top10, x="crop", y="production_mt")

        st.markdown("### 🌧️ Rainfall Range")
        st.line_chart(df_rain.groupby("year")["annual"].mean())

    # ---------------------------------------------------------
    # TAB 3 — Raw Data
    # ---------------------------------------------------------
    with tab3:
        option = st.selectbox("Select Dataset", ["Crop Production", "Rainfall"])

        if option == "Crop Production":
            st.dataframe(df_crop)
        else:
            st.dataframe(df_rain)


# ---------------------------------------------------------
if __name__ == "__main__":
    main()
