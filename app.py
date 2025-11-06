# --------------------------------------------------------
# ✅ Project Samarth - Streamlit App (Older Stable Version)
# --------------------------------------------------------

import streamlit as st
import pandas as pd
import duckdb

# -------------------------------
# ✅ Page Configuration
# -------------------------------
st.set_page_config(
    page_title="Project Samarth – Agriculture Insights",
    page_icon="🌾",
    layout="wide"
)

st.title("🌾 Bharat Samarth – Agriculture Q&A Assistant")
st.markdown("""
This interactive tool answers analytical questions using **Indian crop production** and **rainfall datasets**.  
Ask natural-language questions like:

✅ *Top 5 crops in Maharashtra*  
✅ *Compare rainfall between two states*  
✅ *Trend of wheat over last 5 years in Himachal Pradesh*  
""")

# -------------------------------
# ✅ Load Processed Parquet Data
# -------------------------------

@st.cache_data
def load_data():
    crop = pd.read_parquet("data/crop_production.parquet")
    rain = pd.read_parquet("data/imd_rainfall.parquet")
    return crop, rain

crop_df, rain_df = load_data()

# -------------------------------
# ✅ Initialize DuckDB (In-Memory)
# -------------------------------
con = duckdb.connect(database=":memory:")

con.register("crop", crop_df)
con.register("rainfall", rain_df)

# -------------------------------
# ✅ Natural-Language Query Parser
# -------------------------------
import re

def samarth_plan(question):
    q = question.lower().strip()

    # -----------------------------------------
    # 1️⃣ Top N crops in a state
    # -----------------------------------------
    m = re.search(r"top\s+(\d+)\s+crops\s+in\s+([\w\s]+)", q)
    if m:
        n, state = m.groups()
        sql = f"""
            SELECT crop, SUM(production_mt) AS total_prod
            FROM crop
            WHERE lower(state) = '{state.strip()}'
            GROUP BY crop
            ORDER BY total_prod DESC
            LIMIT {int(n)};
        """
        return {"intent": "top_crops", "sql": sql, "state": state.title(), "n": int(n)}

    # -----------------------------------------
    # 2️⃣ Trend of crop over last N years
    # -----------------------------------------
    m = re.search(r"trend.*of\s+([\w\s]+).*last\s+(\d+)\s+years.*in\s+([\w\s]+)", q)
    if m:
        crop_name, n, state = m.groups()
        max_year = crop_df["year"].max()
        min_year = max_year - int(n) + 1

        sql_prod = f"""
            SELECT year, SUM(production_mt) AS total_prod
            FROM crop
            WHERE lower(crop) LIKE '%{crop_name.strip()}%'
              AND lower(state) = '{state.strip()}'
              AND year BETWEEN {min_year} AND {max_year}
            GROUP BY year ORDER BY year;
        """

        sql_rain = f"""
            SELECT year, AVG(annual) AS avg_rain
            FROM rainfall
            WHERE lower(state) = '{state.strip()}'
              AND year BETWEEN {min_year} AND {max_year}
            GROUP BY year ORDER BY year;
        """

        return {
            "intent": "trend",
            "crop": crop_name.title(),
            "state": state.title(),
            "years": (min_year, max_year),
            "sql_prod": sql_prod,
            "sql_rain": sql_rain
        }

    # -----------------------------------------
    # 3️⃣ Compare rainfall between two states
    # -----------------------------------------
    m = re.search(r"compare.*rainfall.*between\s+([\w\s]+)\s+and\s+([\w\s]+)", q)
    if m:
        s1, s2 = m.groups()

        sql = f"""
            SELECT state, AVG(annual) AS avg_rain
            FROM rainfall
            WHERE lower(state) IN ('{s1.strip()}', '{s2.strip()}')
            GROUP BY state;
        """

        return {"intent": "compare_rain", "sql": sql, "states": (s1.title(), s2.title())}

    return {"intent": "unknown"}

# -------------------------------
# ✅ Execute and Build Answers
# -------------------------------
def execute_and_synthesize(plan):

    if plan["intent"] == "unknown":
        return "⚠️ I couldn't interpret that. Try asking in a simpler way.", None

    # ✅ 1. Top N crops
    if plan["intent"] == "top_crops":
        df = con.execute(plan["sql"]).df()
        text = f"### 🌾 Top {plan['n']} Crops in {plan['state']}\n"
        for _, r in df.iterrows():
            text += f"- **{r['crop']}** → {int(r['total_prod'])} tonnes\n"
        return text, df

    # ✅ 2. Crop Trend
    if plan["intent"] == "trend":
        df_prod = con.execute(plan["sql_prod"]).df()
        df_rain = con.execute(plan["sql_rain"]).df()

        merged = df_prod.merge(df_rain, on="year", how="left")
        corr = merged["total_prod"].corr(merged["avg_rain"])

        text = f"""
### 📈 Trend for **{plan['crop']}** in **{plan['state']}**
**Years:** {plan['years'][0]} – {plan['years'][1]}

✅ Correlation between production & rainfall: **{round(corr, 2) if corr==corr else 'N/A'}**
"""

        return text, merged

    # ✅ 3. Rainfall comparison
    if plan["intent"] == "compare_rain":
        df = con.execute(plan["sql"]).df()
        s1, s2 = plan["states"]

        text = f"### 🌦 Rainfall Comparison: {s1} vs {s2}\n"
        for _, r in df.iterrows():
            text += f"- **{r['state']}** → {round(r['avg_rain'], 2)} mm\n"
        return text, df

    return "⚠️ Something went wrong.", None

# -------------------------------
# ✅ Streamlit UI
# -------------------------------
st.subheader("💬 Ask a Question")
user_q = st.text_input("Type your query here:")

if user_q:
    with st.spinner("Analyzing your question..."):
        plan = samarth_plan(user_q)
        answer, df_out = execute_and_synthesize(plan)

    st.markdown(answer)

    if df_out is not None and len(df_out) > 0:
        st.dataframe(df_out, use_container_width=True)

st.markdown("---")
st.markdown("Built with ❤️ for Bharat Digital Fellowship Program.")

