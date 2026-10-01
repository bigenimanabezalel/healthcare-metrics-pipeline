"""
streamlit_app.py

Healthcare Metrics Dashboard -- AWS-only architecture, no Snowflake.
Google Drive -> Lambda -> S3 raw -> Glue ETL -> S3 processed (Parquet) ->
this Streamlit app, which queries the Parquet files directly with DuckDB.

DuckDB is an embedded, in-process SQL engine -- no separate server, no
Athena/Glue Catalog needed. Its `httpfs` extension lets `read_parquet()`
point straight at an s3:// path, so the SQL below is close to the
Snowflake/Athena version, just swapping the FROM clause.

Requirements:
    pip install streamlit duckdb pandas plotly

Run:
    streamlit run streamlit_app.py

AWS credentials: DuckDB's httpfs picks up the standard AWS credential chain
(environment variables, ~/.aws/credentials, or an IAM role if deployed on
AWS). No AWS keys need to be hardcoded here -- set AWS_ACCESS_KEY_ID /
AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION as environment variables, or
put them in .streamlit/secrets.toml as:
    [aws]
    aws_access_key_id = "..."
    aws_secret_access_key = "..."
    region = "us-east-1"

ASSUMED SCHEMA (from Glue's column-standardization step -- adjust the
column names below if your actual Glue job outputs something different):

  pbj_daily_nurse_staffing.parquet:
    provnum, workdate, mdscensus,
    hrs_rn, hrs_rn_emp, hrs_rn_ctr,
    hrs_lpn, hrs_lpn_emp, hrs_lpn_ctr,
    hrs_cna, hrs_cna_emp, hrs_cna_ctr

  nh_providerinfo.parquet:
    ccn, provider_name, state, provider_type,
    certified_beds, total_nursing_staff_turnover, rn_turnover

  qrp_provider_data.parquet (long format -- one row per ccn per measure):
    ccn, measure_code, score, start_date, end_date
"""

import duckdb
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Healthcare Staffing Metrics", layout="wide")

# --- S3 locations of the 3 Parquet files Glue produces ---
BUCKET = "de-healthcare-project-bucket"
PBJ_PATH = f"s3://{BUCKET}/processed/pbj_daily_nurse_staffing.parquet"
PROVIDER_PATH = f"s3://{BUCKET}/processed/nh_providerinfo.parquet"
QRP_PATH = f"s3://{BUCKET}/processed/qrp_provider_data.parquet"


@st.cache_resource
def get_connection():
    """One DuckDB connection per session, with httpfs loaded once."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    region = st.secrets.get("aws", {}).get("region", "us-east-1")
    con.execute(f"SET s3_region='{region}';")
    # If credentials are in secrets.toml rather than the environment/IAM role:
    if "aws" in st.secrets:
        con.execute(f"SET s3_access_key_id='{st.secrets['aws']['aws_access_key_id']}';")
        con.execute(f"SET s3_secret_access_key='{st.secrets['aws']['aws_secret_access_key']}';")
    return con


@st.cache_data(ttl=600)
def run_query(sql: str):
    con = get_connection()
    return con.execute(sql).df()


@st.cache_data(ttl=600)
def get_states() -> list:
    df = run_query(f"select distinct state from read_parquet('{PROVIDER_PATH}') order by state")
    return df["state"].tolist()


# --- Sidebar filters -- these parameterize every query below ---
st.sidebar.header("Filters")
states = get_states()
selected_states = st.sidebar.multiselect("State", states, default=states[:5])

if not selected_states:
    st.warning("Select at least one state.")
    st.stop()

state_list_sql = ", ".join(f"'{s}'" for s in selected_states)  # small closed list from a dropdown, not raw user text

# ---------------------------------------------------------------------------
# Selected metric 1: Total nursing hours worked, by hospital/state/month
# ---------------------------------------------------------------------------
st.header("Total nursing hours worked, by state and month")

total_hours_sql = f"""
    select
        pi.state,
        date_trunc('month', pbj.workdate) as month,
        sum(pbj.hrs_rn + pbj.hrs_lpn + pbj.hrs_cna) as total_hours
    from read_parquet('{PBJ_PATH}') pbj
    join read_parquet('{PROVIDER_PATH}') pi on pbj.provnum = pi.ccn
    where pi.state in ({state_list_sql})
    group by pi.state, month
    order by month
"""
total_hours_df = run_query(total_hours_sql)
total_hours_pivot = total_hours_df.pivot(index="month", columns="state", values="total_hours")
st.line_chart(total_hours_pivot)
st.caption(
    "Covers only the months present in PBJ_Daily_Nurse_Staffing_Q2_2024.csv "
    "(one quarter) -- not a full year of history."
)

# ---------------------------------------------------------------------------
# Selected metric 2: Hospital occupancy rate trend, by month
# ---------------------------------------------------------------------------
st.header("Occupancy rate trend")

occupancy_trend_sql = f"""
    select
        date_trunc('month', pbj.workdate) as month,
        avg(pbj.mdscensus) as avg_census
    from read_parquet('{PBJ_PATH}') pbj
    join read_parquet('{PROVIDER_PATH}') pi on pbj.provnum = pi.ccn
    where pi.state in ({state_list_sql})
    group by month
    order by month
"""
trend_df = run_query(occupancy_trend_sql)
st.line_chart(trend_df.set_index("month")["avg_census"])

# ---------------------------------------------------------------------------
# Selected metric 3 / Q1: Staffing levels vs. occupancy rate (scatter)
# ---------------------------------------------------------------------------
st.header("Staffing levels vs. occupancy rate")

staffing_vs_occupancy_sql = f"""
    select
        pi.provider_name,
        pi.state,
        pi.provider_type,
        avg((pbj.hrs_rn + pbj.hrs_lpn + pbj.hrs_cna) / nullif(pbj.mdscensus, 0)) as hppd,
        avg(pbj.mdscensus) / nullif(pi.certified_beds, 0) as occupancy_rate
    from read_parquet('{PBJ_PATH}') pbj
    join read_parquet('{PROVIDER_PATH}') pi on pbj.provnum = pi.ccn
    where pi.state in ({state_list_sql})
    group by pi.provider_name, pi.state, pi.provider_type, pi.certified_beds
"""
so_df = run_query(staffing_vs_occupancy_sql)
fig_scatter = px.scatter(
    so_df, x="occupancy_rate", y="hppd", color="state", hover_data=["provider_name"],
    labels={"occupancy_rate": "Occupancy rate", "hppd": "Nursing hours per resident per day"},
)
st.plotly_chart(fig_scatter, use_container_width=True)
correlation = so_df["occupancy_rate"].corr(so_df["hppd"])
st.caption(f"Correlation coefficient: {correlation:.2f} (closer to 0 = weaker relationship)")

# ---------------------------------------------------------------------------
# Selected metric 4: Correlation between nurse staffing and readmission rate
# The QRP file is in LONG format (one row per ccn per measure code), so this
# needs a WHERE filter on measure code before joining.
# ---------------------------------------------------------------------------
st.header("Staffing levels vs. readmission rate")

readmission_sql = f"""
    with staffing as (
        select
            provnum,
            avg((hrs_rn + hrs_lpn + hrs_cna) / nullif(mdscensus, 0)) as hppd
        from read_parquet('{PBJ_PATH}')
        group by provnum
    ),
    readmission as (
        select ccn, try_cast(score as double) as readmission_rate
        from read_parquet('{QRP_PATH}')
        where measure_code = 'S_004_01_PPR_PD_RSRR'
    )
    select
        pi.provider_name,
        pi.state,
        s.hppd,
        r.readmission_rate
    from staffing s
    join read_parquet('{PROVIDER_PATH}') pi on s.provnum = pi.ccn
    join readmission r on s.provnum = r.ccn
    where pi.state in ({state_list_sql})
      and r.readmission_rate is not null
"""
readmission_df = run_query(readmission_sql)
if readmission_df.empty:
    st.info(
        "No matching rows -- double check the exact measure_code value in "
        "your Parquet file (S_004_01_PPR_PD_RSRR is the documented code, "
        "but confirm it matches what Glue actually wrote)."
    )
else:
    fig_readmission = px.scatter(
        readmission_df, x="hppd", y="readmission_rate", color="state", hover_data=["provider_name"],
        labels={"hppd": "Nursing hours per resident per day", "readmission_rate": "Risk-standardized readmission rate"},
    )
    st.plotly_chart(fig_readmission, use_container_width=True)
    readmit_corr = readmission_df["hppd"].corr(readmission_df["readmission_rate"])
    st.caption(f"Correlation coefficient: {readmit_corr:.2f}")

# ---------------------------------------------------------------------------
# Selected metric 5: Ratio of permanent (employed) to contract staff
# ---------------------------------------------------------------------------
st.header("Contract vs. employed staffing ratio")

ctr_ratio_sql = f"""
    select
        pi.state,
        sum(pbj.hrs_rn_ctr + pbj.hrs_lpn_ctr + pbj.hrs_cna_ctr) as contract_hours,
        sum(pbj.hrs_rn_emp + pbj.hrs_lpn_emp + pbj.hrs_cna_emp) as employed_hours
    from read_parquet('{PBJ_PATH}') pbj
    join read_parquet('{PROVIDER_PATH}') pi on pbj.provnum = pi.ccn
    where pi.state in ({state_list_sql})
    group by pi.state
"""
ctr_df = run_query(ctr_ratio_sql)
ctr_df["contract_ratio"] = ctr_df["contract_hours"] / (ctr_df["contract_hours"] + ctr_df["employed_hours"])
st.bar_chart(ctr_df.set_index("state")["contract_ratio"])

# ---------------------------------------------------------------------------
# Q3: Average staffing levels by state and hospital type (heatmap)
# ---------------------------------------------------------------------------
st.header("Q3: Average staffing levels by state and hospital type")

heatmap_sql = f"""
    select
        pi.state,
        pi.provider_type,
        avg((pbj.hrs_rn + pbj.hrs_lpn + pbj.hrs_cna) / nullif(pbj.mdscensus, 0)) as hppd
    from read_parquet('{PBJ_PATH}') pbj
    join read_parquet('{PROVIDER_PATH}') pi on pbj.provnum = pi.ccn
    where pi.state in ({state_list_sql})
    group by pi.state, pi.provider_type
"""
heatmap_df = run_query(heatmap_sql)
heatmap_pivot = heatmap_df.pivot(index="state", columns="provider_type", values="hppd")
fig_heatmap = px.imshow(
    heatmap_pivot, color_continuous_scale="YlOrRd", aspect="auto",
    labels=dict(color="Hours per resident per day"),
)
st.plotly_chart(fig_heatmap, use_container_width=True)

# ---------------------------------------------------------------------------
# Nursing staff turnover, by state (pre-computed column, no join needed)
# ---------------------------------------------------------------------------
st.header("Nursing staff turnover, by state")

turnover_sql = f"""
    select
        state,
        avg(total_nursing_staff_turnover) as avg_total_turnover,
        avg(rn_turnover) as avg_rn_turnover
    from read_parquet('{PROVIDER_PATH}')
    where state in ({state_list_sql})
    group by state
    order by avg_total_turnover desc
"""
turnover_df = run_query(turnover_sql)
st.bar_chart(turnover_df.set_index("state")[["avg_total_turnover", "avg_rn_turnover"]])

# ---------------------------------------------------------------------------
# Q2 and Q4: NOT reliably calculable with this data -- shown transparently.
# ---------------------------------------------------------------------------
st.header("Data limitations (Q2 and Q4)")

st.warning(
    "**Q2 (highest overtime hours) cannot be directly answered.** The PBJ "
    "file reports total hours per facility per day, not per employee or "
    "per shift -- there is no way to distinguish a single nurse's overtime "
    "hours from ordinary hours split across multiple staff."
)

st.error(
    "**Q4 (length of stay trends) cannot be answered at all.** No field "
    "across any of the 19 available files records a resident's length of "
    "stay or admission/discharge dates. The census trend chart above is "
    "the closest available substitute -- it shows occupancy patterns over "
    "time, not a length-of-stay measure."
)

st.info(
    "**Entire Cost Metrics category (payroll, cost per stay, overtime cost "
    "%, revenue vs. payroll) is not calculable.** No wage, billing, or "
    "revenue data exists in any of the 19 files.\n\n"
    "**Department-level breakdowns are impossible everywhere requested** -- "
    "PBJ has no unit/ward/department field.\n\n"
    "See metrics_feasibility_audit.md for the full metric-by-metric "
    "reasoning across all 22 requested metrics."
)
