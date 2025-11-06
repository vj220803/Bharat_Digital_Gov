# --------------------------------------------------------
# Bharat Samarth – Older Stable App (Crop + Rainfall only)
# Columns used:
#   CROP: state, district, crop, production_mt, year
#   RAIN: state, year, jan,feb,...,dec, annual, jf, mam, jjas, ond
# Reads ONLY Parquet from data/ and guards all queries.
# --------------------------------------------------------

import os
import re
import streamlit as st
import pandas as pd
import duckdb

st.set_page_config(page_title="Bharat Samarth – Q&A (Stable)", page_icon="🌾", layout="wide")
st.title("🌾 Bharat Samarth — Agriculture Q&A (Stable Version)")
st.caption("Uses cleaned Crop + Rainfall parquet files with fixed schemas.")

DATA_DIR = "data"
CROP_PATH = os.path.join(DATA_DIR, "crop_production.parquet")
RAIN_PATH = os.path.join(DATA_DIR, "imd_rainfall.parquet")

REQUIRED_CROP_COLS = {"state","district","crop","production_mt","year"}
REQUIRED_RAIN_COLS = {"state","year","annual"}  # monthly exist but we only require these

# ----------------------------
# Helpers
# ----------------------------
def load_parquet_or_stop(path, label):
    if not os.path.exists(path):
        st.error(f"❌ Missing file: `{path}`. Please add it to your repo.")
        st.stop()
    try:
        df = pd.read_parquet(path)
        return df
    except Exception as e:
        st.error(f"❌ Could not read {label} from `{path}`: {e}")
        st.stop()

def check_columns(df, required, label):
    missing = required - set(df.columns)
    if missing:
        st.error(f"❌ The {label} file is missing columns: {sorted(list(missing))}")
        st.stop()

def norm_text(s: str) -> str:
    return str(s).strip()

def safe_like(s: str) -> str:
    return s.replace("'", "''")

# ----------------------------
# Load data
# ----------------------------
crop_df = load_parquet_or_stop(CROP_PATH, "Crop")
rain_df = load_parquet_or_stop(RAIN_PATH, "Rainfall")

# Ensure exactly your columns are present
check_columns(crop_df, REQUIRED_CROP_COLS, "CROP (parquet)")
check_columns(rain_df, REQUIRED_RAIN_COLS, "RAINFALL (parquet)")

# Light cleanup (without renaming your final columns)
crop_df["state"] = crop_df["state"].apply(norm_text)
crop_df["crop"] = crop_df["crop"].apply(norm_text)
# ensure numeric
crop_df["year"] = pd.to_numeric(crop_df["year"], errors="coerce")
crop_df["production_mt"] = pd.to_numeric(crop_df["production_mt"], errors="coerce")

rain_df["state"] = rain_df["state"].apply(norm_text)
rain_df["year"] = pd.to_numeric(rain_df["year"], errors="coerce")
rain_df["annual"] = pd.to_numeric(rain_df["annual"], errors="coerce")

# Drop rows missing critical keys
crop_df = crop_df.dropna(subset=["state","year"]).reset_index(drop=True)
rain_df = rain_df.dropna(subset=["state","year"]).reset_index(drop=True)

# ----------------------------
# Show dataset status
# ----------------------------
with st.expander("🧪 Dataset status", expanded=False):
    st.write("**Crop columns:**", list(crop_df.columns))
    st.write("**Rain columns:**", list(rain_df.columns))
    st.write(f"**Crop rows:** {len(crop_df)} | **Rain rows:** {len(rain_df)}")
    st.write("**Distinct crop states:**", sorted(crop_df["state"].dropna().unique().tolist())[:20])
    st.write("**Distinct rain states:**", sorted(rain_df["state"].dropna().unique().tolist())[:20])

# ----------------------------
# DuckDB in-memory
# ----------------------------
con = duckdb.connect(database=":memory:")
con.register("crop", crop_df)
con.register("rain", rain_df)

# ----------------------------
# Planner: supports 3 intents
# ----------------------------
# 1) Top N crops in <State>
# 2) Trend of <Crop> over last N years in <State> (joins with rainfall)
# 3) Compare rainfall between <State1> and <State2> (avg across all years)

def plan_query(q: str):
    ql = q.lower().strip()

    # Top N crops in <state>
    m = re.search(r"top\s+(\d+)\s+crops\s+in\s+([\w\s&.-]+)$", ql)
    if m:
        n, state = m.groups()
        n = int(n)
        state_like = safe_like(state.strip().lower())
        sql = f"""
            SELECT crop, SUM(production_mt) AS total_prod
            FROM crop
            WHERE lower(state) = '{state_like}'
            GROUP BY crop
            ORDER BY total_prod DESC
            LIMIT {n};
        """
        return {"intent":"top_crops", "sql":sql, "n":n, "state":state.strip().title()}

    # Trend of <crop> over last N years in <state>
    m = re.search(r"trend\s+of\s+([\w\s&.-]+)\s+over\s+last\s+(\d+)\s+years\s+in\s+([\w\s&.-]+)$", ql)
    if m:
        crop_name, n, state = m.groups()
        n = int(n)
        # derive max_year from rain or crop (whichever is present)
        max_year_candidates = []
        if "year" in crop_df.columns and not crop_df["year"].dropna().empty:
            max_year_candidates.append(int(crop_df["year"].max()))
        if "year" in rain_df.columns and not rain_df["year"].dropna().empty:
            max_year_candidates.append(int(rain_df["year"].max()))
        if not max_year_candidates:
            return {"intent":"unknown"}
        max_year = max(max_year_candidates)
        min_year = max_year - n + 1

        crop_like = safe_like(crop_name.strip().lower())
        state_like = safe_like(state.strip().lower())

        sql_prod = f"""
            SELECT year, SUM(production_mt) AS total_prod
            FROM crop
            WHERE lower(crop) LIKE '%{crop_like}%'
              AND lower(state) = '{state_like}'
              AND year BETWEEN {min_year} AND {max_year}
            GROUP BY year ORDER BY year;
        """
        sql_rain = f"""
            SELECT year, AVG(annual) AS avg_rain
            FROM rain
            WHERE lower(state) = '{state_like}'
              AND year BETWEEN {min_year} AND {max_year}
            GROUP BY year ORDER BY year;
        """
        return {"intent":"trend", "crop":crop_name.strip().title(), "state":state.strip().title(),
                "years":(min_year,max_year), "sql_prod":sql_prod, "sql_rain":sql_rain}

    # Compare rainfall between <state1> and <state2>
    m = re.search(r"compare\s+rainfall\s+between\s+([\w\s&.-]+)\s+and\s+([\w\s&.-]+)$", ql)
    if m:
        s1, s2 = m.groups()
        s1l, s2l = safe_like(s1.strip().lower()), safe_like(s2.strip().lower())
        sql = f"""
            SELECT state, AVG(annual) AS avg_rain
            FROM rain
            WHERE lower(state) IN ('{s1l}','{s2l}')
            GROUP BY state;
        """
        return {"intent":"compare_rain", "sql":sql, "s1":s1.strip().title(), "s2":s2.strip().title()}

    return {"intent":"unknown"}

# ----------------------------
# Executor + Synthesizer
# ----------------------------
def execute_and_answer(plan):
    if plan["intent"] == "unknown":
        return "⚠️ I couldn't interpret that. Try:\n- `Top 5 crops in Himachal Pradesh`\n- `Trend of wheat over last 5 years in Himachal Pradesh`\n- `Compare rainfall between Maharashtra and Karnataka`", None

    if plan["intent"] == "top_crops":
        df = con.execute(plan["sql"]).df()
        if df.empty:
            return f"ℹ️ No crop data for **{plan['state']}**.", df
        lines = [f"### 🌾 Top {plan['n']} Crops in {plan['state']}"]
        for _, r in df.iterrows():
            lines.append(f"- **{r['crop']}** — {int(r['total_prod'])} t")
        return "\n".join(lines), df

    if plan["intent"] == "trend":
        dfp = con.execute(plan["sql_prod"]).df()
        dfr = con.execute(plan["sql_rain"]).df()
        merged = dfp.merge(dfr, on="year", how="left")
        corr = merged["total_prod"].corr(merged["avg_rain"]) if not merged.empty else None
        corr_txt = "N/A" if (corr is None or pd.isna(corr)) else round(float(corr), 2)
        txt = f"""### 📈 Trend: **{plan['crop']}** in **{plan['state']}**
**Years:** {plan['years'][0]}–{plan['years'][1]}
**Correlation (production vs rainfall):** {corr_txt}
"""
        if merged.empty:
            txt += "\nℹ️ No overlapping data points found for that period."
        return txt, merged

    if plan["intent"] == "compare_rain":
        df = con.execute(plan["sql"]).df()
        if df.empty:
            return "ℹ️ No rainfall data found for those states.", df
        lines = [f"### 🌦 Rainfall Comparison — {plan['s1']} vs {plan['s2']}"]
        for _, r in df.iterrows():
            lines.append(f"- **{r['state']}** — {round(r['avg_rain'],1)} mm (avg across available years)")
        return "\n".join(lines), df

    return "⚠️ Something went wrong.", None

# ----------------------------
# UI
# ----------------------------
st.subheader("💬 Ask a question")
st.write("Examples:")
st.code("Top 5 crops in Himachal Pradesh", language="text")
st.code("Trend of wheat over last 5 years in Himachal Pradesh", language="text")
st.code("Compare rainfall between Maharashtra and Karnataka", language="text")

q = st.text_input("Type your query:")
if q:
    with st.spinner("Thinking..."):
        plan = plan_query(q)
        answer, df = execute_and_answer(plan)
    st.markdown(answer)
    if isinstance(df, pd.DataFrame) and not df.empty:
        st.dataframe(df, use_container_width=True)

st.markdown("---")
st.caption("This stable app reads only: data/crop_production.parquet & data/imd_rainfall.parquet")
