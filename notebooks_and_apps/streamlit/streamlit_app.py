# streamlit_app.py
import streamlit as st
import bauplan
import pandas as pd
from datetime import date
import matplotlib.pyplot as plt

# 1) Initialize Bauplan client
#    The client will use your local profile or BAUPLAN_API_KEY for authentication.
#    We'll also capture the username so we can scope a branch per user.
client = bauplan.Client()
username = client.info().user.username

# 2) Define a user-scoped branch
#    Apps should never query `main` directly. By running against a personal branch,
#    you keep exploration isolated from production.
exploration_branch = f"{username}.exploration_app"

# Sidebar for UI inputs
st.sidebar.header("Filters")

# 3) Date inputs
#    The sandbox dataset contains fhvhv taxi rides for 2023.
#    Default to the month of January 2023 for reproducibility.
start_date = st.sidebar.date_input("Start date", value=date(2023, 1, 1))
end_date   = st.sidebar.date_input("End date",   value=date(2023, 1, 31))

# Guard against invalid ranges
if start_date > end_date:
    st.warning("Start date must be <= end date.")
    st.stop()

# 4) Query function (cached)
#    We wrap the SQL query in @st.cache_data so Streamlit reuses results
#    instead of re-running the query every time the page refreshes.
@st.cache_data(show_spinner="Running query…")
def run_query(start: str, end: str, ref: str) -> pd.DataFrame:
    query = f"""
        SELECT
          DATE_TRUNC('day', pickup_datetime) AS dt,
          COUNT(*) AS rides
        FROM taxi_fhvhv
        WHERE pickup_datetime BETWEEN '{start}' AND '{end}'
        GROUP BY dt
        ORDER BY dt ASC
    """
    return client.query(query, ref=ref).to_pandas()

# Execute the query against the sandbox table on our branch
df = run_query(start_date.isoformat(), end_date.isoformat(), exploration_branch)

st.subheader("Rides per day")

# Stop early if the query returned no rows
if df.empty:
    st.info("No rows for the selected date range.")
    st.stop()

# 5) Normalize data types
#    Ensure datetime and numeric types are consistent for plotting.
df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
df["rides"] = pd.to_numeric(df["rides"], errors="coerce").fillna(0).astype("int64")

# 6) Display the result table
st.dataframe(df, use_container_width=True)

# 7) Create a bar chart with matplotlib
#    We render server-side with matplotlib, then embed the figure in Streamlit.
labels = df["dt"].dt.strftime("%Y-%m-%d")
values = df["rides"].values

fig, ax = plt.subplots(figsize=(max(6, len(df) * 0.25), 4))
ax.bar(labels, values)
ax.set_xlabel("Date")
ax.set_ylabel("Rides")
ax.set_title(f"Rides per day ({start_date} → {end_date})")
ax.tick_params(axis="x", labelrotation=45)
fig.tight_layout()

# 8) Render the chart in the app
st.pyplot(fig, clear_figure=True)