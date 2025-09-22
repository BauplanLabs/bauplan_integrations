# marimo_app.py
import marimo

app = marimo.App()


# 1) Imports
#    - marimo as mo for UI/layout
#    - bauplan for querying your lakehouse
#    - pandas for dataframe ops
#    - matplotlib for plotting
@app.cell
def _():
    import marimo as mo
    import bauplan
    import pandas as pd
    from datetime import date
    import matplotlib.pyplot as plt
    return mo, bauplan, pd, date, plt


# 2) Initialize Bauplan client and define a user-scoped branch
#    Apps should never query `main` directly. Run against a personal branch
#    to keep exploration isolated from production.
@app.cell
def _(bauplan):
    client = bauplan.Client()
    username = client.info().user.username
    exploration_branch = f"{username}.exploration_app"
    return client, username, exploration_branch


# Sidebar for UI inputs
# 3) Date inputs
#    The sandbox dataset contains fhvhv taxi rides for 2023.
#    Default to the month of January 2023 for reproducibility.
@app.cell
def _(mo, date):
    date_range = mo.ui.date_range(
        start=date(2023, 1, 1),
        stop=date(2023, 12, 31),
        value=(date(2023, 1, 1), date(2023, 1, 31)),
    )
    # Render a sidebar with controls
    mo.sidebar(
        mo.vstack([
            mo.md("### Filters"),
            date_range
        ])
    )
    return date_range


# 4) Query function (cached)
#    Wrap the SQL query in @mo.cache so expensive results are reused
#    when inputs do not change.
@app.cell
def _(mo, client):
    @mo.cache  # in-memory caching
    def run_query(start: str, end: str, ref: str):
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
    return run_query


# Execute the query against the sandbox table on our branch
@app.cell
def _(date_range, exploration_branch, run_query):
    start_date, end_date = date_range.value
    df = run_query(start_date.isoformat(), end_date.isoformat(), exploration_branch)
    return df, start_date, end_date


# 5) Normalize data types
#    Ensure datetime and numeric types are consistent for plotting.
@app.cell
def _(pd, df):
    df_norm = df.copy()
    if not df_norm.empty:
        df_norm["dt"] = pd.to_datetime(df_norm["dt"], errors="coerce")
        df_norm["rides"] = (
            pd.to_numeric(df_norm["rides"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
    return df_norm


# 6) Display the result table
@app.cell
def _(mo, df_norm):
    header = mo.md("## Rides per day")
    body = mo.md("No rows for the selected date range.") if df_norm.empty else mo.ui.table(df_norm)
    mo.vstack([header, body])
    return


# 7) Create a bar chart with matplotlib and embed it in Marimo
@app.cell
def _(mo, df_norm, start_date, end_date, plt):
    out = mo.md("No chart: no rows.") if df_norm.empty else None
    if not df_norm.empty:
        labels = df_norm["dt"].dt.strftime("%Y-%m-%d")
        values = df_norm["rides"].values
        fig, ax = plt.subplots(figsize=(max(6, len(df_norm) * 0.25), 4))
        ax.bar(labels, values)
        ax.set_xlabel("Date"); ax.set_ylabel("Rides")
        ax.set_title(f"Rides per day ({start_date} \u2192 {end_date})")
        ax.tick_params(axis="x", labelrotation=45)
        fig.tight_layout()
        out = mo.mpl.interactive(fig)

    out  # last expression renders


if __name__ == "__main__":
    app.run()