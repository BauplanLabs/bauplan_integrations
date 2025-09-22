# Bauplan Integrations (Local Examples)

This repository contains runnable, minimal examples that show how to integrate **Bauplan** with popular orchestrators and interactive apps. Each example is intentionally small so you can copy it into your environment and adapt to your project.

The repo includes:

- **Bauplan pipelines** used by the examples (`bauplan_pipelines/`).
- **Orchestrator integrations**: Airflow, Prefect, Dagster, Temporal, DBOS (`orchestrators/`).
- **Interactive apps**: Streamlit, Marimo, Jupyter (`notebooks_and_apps/`).

All examples follow the same pattern: the orchestrator or app handles the UI, scheduling, and logging, while **Bauplan** runs the data work (pipelines, queries, branching) against your lakehouse.

> Docs: https://docs.bauplanlabs.com/ (see “Concepts → Projects,” “Guides → Parameters,” and “Guides → Secrets”).

---

## Prerequisites

- **Python** 3.10 or newer.
- A **Bauplan API key** or local profile.
  - The client resolves credentials in this order: `BAUPLAN_API_KEY` → `BAUPLAN_PROFILE` → `~/.bauplan/config.yml`.
- A Bauplan sandbox environment with example tables like `taxi_fhvhv` and `taxi_zones`, or substitute your own tables.
- Per-example dependencies are listed in each subfolder’s `requirements.txt`.

Authentication example:

```python
import bauplan
client = bauplan.Client(api_key="YOUR_KEY")  # overrides environment/profile
```

Set via environment if preferred:

```bash
bauplan config set api_key <your_key>
```


## Quickstart

Create a clean virtual environment and install dependencies for the example you want to run.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r orchestrators/prefect/requirements.txt  # or airflow/dagster/temporal/dbos, or notebooks_and_apps/streamlit
```

Pick a pipeline to run:

- `bauplan_pipelines/simple_pipeline` – no parameters.
- `bauplan_pipelines/pipeline_with_parameters` – uses `$start_time` in `bauplan_project.yml`.

Most examples construct a user-scoped branch name like `{username}.{suffix}`. This keeps app and test runs isolated from `main`.

---

## Running the examples

### 1) Prefect

Run a Bauplan pipeline:

```bash
pip install -r orchestrators/prefect/requirements.txt
python orchestrators/prefect/bauplan_prefect_flow.py
```

Run with parameters:

```bash
python orchestrators/prefect/bauplan_prefect_with_param.py
# Edit pipeline_path, branch_suffix, and parameters in __main__ if needed
```

### 2) Airflow

- Requires Airflow 2.10+ or 3.0+ and a configured Airflow environment.
- Install requirements, then copy the DAG files into your `$AIRFLOW_HOME/dags` folder or load them as part of your plugins/module.

```bash
pip install -r orchestrators/airflow/requirements.txt
# Copy DAGs
cp orchestrators/airflow/bauplan_airflow3_flow.py  $AIRFLOW_HOME/dags/
cp orchestrators/airflow/bauplan_airflow3_with_param.py  $AIRFLOW_HOME/dags/
airflow webserver
airflow scheduler
```

In each file, update `project_dir` to point at one of the folders under `bauplan_pipelines/` before deploying the DAGs.

### 3) Dagster

You can run the example directly as a Python script (the file includes an `if __name__ == "__main__"` block with `execute_in_process`), or load it with `dagster dev`.

**Run the script:**

```bash
pip install -r orchestrators/dagster/requirements.txt
python orchestrators/dagster/bauplan_dagster_flow.py
python orchestrators/dagster/bauplan_dagster_with_param.py
```

**Or use the UI:**

```bash
export PYTHONPATH="$PWD/orchestrators/dagster:$PYTHONPATH"
dagster dev -m bauplan_dagster_flow
```

Update `project_dir`, `bauplan_branch_suffix`, and `parameters` in the file or in `run_config` as needed.

### 4) Temporal

- Requires a Temporal server at `localhost:7233`. For a quick start, use Temporal’s Docker Compose.
- Install requirements, then start the worker and workflow runner.

```bash
pip install -r orchestrators/temporal/requirements.txt

# Basic example (no parameters)
python orchestrators/temporal/workflow_and_worker.py

# With parameters
python orchestrators/temporal/workflow_and_worker_with_param.py
```

Activities call `bauplan.Client().run(...)` and raise on non-success job status. Update `project_dir`, `bauplan_branch_suffix`, and `parameters` before running.

### 5) DBOS

The examples default to SQLite via DBOS Python. To use Postgres, set `DBOS_SYSTEM_DATABASE_URL`.

```bash
pip install -r orchestrators/dbos/requirements.txt

# Basic example
python orchestrators/dbos/dbos_bauplan_flow.py

# With parameters
python orchestrators/dbos/dbos_bauplan_with_param.py
```

### 6) Streamlit app

A small UI for ad hoc exploration against an isolated branch.

```bash
pip install -r notebooks_and_apps/streamlit/requirements.txt
streamlit run notebooks_and_apps/streamlit/streamlit_app.py
```

The app reads from `taxi_fhvhv` and plots simple aggregations. Adjust the query inside the file to target your own tables if needed.

### 7) Marimo app

An interactive notebook-style app with reactive cells and cached queries.

```bash
pip install -r notebooks_and_apps/marimo/requirements.txt
marimo run notebooks_and_apps/marimo/marimo_app.py
```

### 8) Jupyter notebooks

Open the notebook(s) under `notebooks_and_apps/jupyter/` in JupyterLab or VS Code. The examples demonstrate the same branch pattern used by the apps above.

---

## Parameters

`bauplan_pipelines/pipeline_with_parameters/bauplan_project.yml` defines a project-level parameter:

```yaml
parameters:
  start_time:
    type: str
    default: "2022-12-15T00:00:00-05:00"
```

You can override it at runtime from any integration:

```python
state = client.run(
    project_dir="path/to/pipeline_with_parameters",
    ref=f"{username}.your_branch",
    parameters={"start_time": "2023-01-01T00:00:00-05:00"},
)
```

See docs: https://docs.bauplanlabs.com/en/latest/guides/parameters/

Secrets can be passed securely via Bauplan and should not be hardcoded. See: https://docs.bauplanlabs.com/en/latest/guides/secrets/

---

## Tips and conventions

- **Never run apps against `main`**. Use `{username}.suffix` branches to keep experiments isolated.
- **Replace sample tables** (`taxi_fhvhv`, `taxi_zones`) with your own if they are not present in your catalog.

## License

This project is provided with no guarantees under the attached MIT License.
