# dbos_bauplan_flow.py
import os
from typing import Optional, Dict, Any

import bauplan
from dbos import DBOS, DBOSConfig

# 1) Initialize DBOS (uses SQLite by default; see "How to run it locally")
config: DBOSConfig = {
    "name": "bauplan-dbos-example",
    # For production, point to Postgres with DBOS_SYSTEM_DATABASE_URL
    "system_database_url": os.environ.get("DBOS_SYSTEM_DATABASE_URL"),
}
DBOS(config=config)
DBOS.launch()


# 2) Define a DBOS step that performs I/O (calls Bauplan)
@DBOS.step()
def run_bauplan_step(project_dir: str, bauplan_branch_suffix: str,
                     parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Execute a Bauplan project and return a minimal, serializable summary.

    Returns:
        dict with 'job_id' and 'job_status'.
    Raises:
        RuntimeError if the job did not finish with status 'success'.
    """
    client = bauplan.Client()  # build client inside the step (I/O)
    username = client.info().user.username  # get username
    branch = f"{username}.{bauplan_branch_suffix}"  # construct branch name

    state = client.run(project_dir=project_dir, ref=branch, parameters=parameters or {})

    if str(state.job_status).lower() != "success":
        raise RuntimeError(
            f"Bauplan job {state.job_id} ended with status='{state.job_status}'"
        )

    return {"job_id": state.job_id, "job_status": state.job_status}


# 3) (Optional) A tiny reporting step
@DBOS.step()
def report_success(result: Dict[str, Any]) -> None:
    print(f"Bauplan run succeeded (job_id={result['job_id']}, status={result['job_status']})")


# 4) Define a DBOS workflow that calls the steps
@DBOS.workflow()
def bauplan_pipeline_workflow(
    pipeline_dir: str,
    bauplan_branch_suffix: str,
) -> Dict[str, Any]:
    result = run_bauplan_step(pipeline_dir, bauplan_branch_suffix)
    report_success(result)
    return result


if __name__ == "__main__":
    # Call the workflow directly for fast local debugging (runs in-process, durably).
    # You can also start in the background with DBOS.start_workflow(...).  # see docs
    bauplan_pipeline_workflow(
        pipeline_dir="your_bauplan_project", # change it with the path to your bauplan project
        bauplan_branch_suffix="dbos_docs", # change it with your bauplan branch
    )