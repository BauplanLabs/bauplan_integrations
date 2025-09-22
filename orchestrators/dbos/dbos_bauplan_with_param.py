# dbos_bauplan_with_params.py
import os
from typing import Dict, Any, Optional
import bauplan
from dbos import DBOS, DBOSConfig

config: DBOSConfig = {
    "name": "bauplan-dbos-params",
    "system_database_url": os.environ.get("DBOS_SYSTEM_DATABASE_URL"),
}
DBOS(config=config)
DBOS.launch()


@DBOS.step()
def run_with_parameters_step(
    project_dir: str,
    bauplan_branch_suffix: str,
    parameters: Dict[str, Any],
) -> Dict[str, Any]:
    client = bauplan.Client()  # build client inside the step
    username = client.info().user.username
    branch = f"{username}.{bauplan_branch_suffix}"

    state = client.run(project_dir=project_dir, ref=branch, parameters=parameters)

    if str(state.job_status).lower() != "success":
        raise RuntimeError(
            f"Bauplan job {state.job_id} ended with status='{state.job_status}'"
        )
    return {"job_id": state.job_id, "job_status": state.job_status}


@DBOS.workflow()
def run_with_parameters_workflow(
    project_dir: str,
    bauplan_branch_suffix: str,  # your own branch
    parameters: Optional[Dict[str, Any]],  # your arbitrary parameters
) -> Dict[str, Any]:
    return run_with_parameters_step(project_dir, bauplan_branch_suffix, parameters or {})


if __name__ == "__main__":
    run_with_parameters_workflow(
        project_dir="your_bauplan_project", # change it with the path to your bauplan project
        bauplan_branch_suffix="dbos_docs", # change it with your bauplan branch
        parameters={"start_time": "2023-01-01T00:00:00-05:00"},
    )