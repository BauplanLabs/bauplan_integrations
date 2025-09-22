# activities_with_param.py
from temporalio import activity
from typing import Dict, Any

@activity.defn
def run_with_parameters_activity(
    project_dir: str,
    bauplan_branch_suffix: str,
    parameters: Dict[str, Any],
) -> dict:
    # Import inside the activity so it never touches the workflow sandbox
    import bauplan

    client = bauplan.Client()
    username = client.info().user.username
    branch = f"{username}.{bauplan_branch_suffix}"

    state = client.run(project_dir=project_dir, ref=branch, parameters=parameters)

    if str(state.job_status).lower() != "success":
        raise RuntimeError(
            f"Bauplan job {state.job_id} ended with status='{state.job_status}'"
        )

    return {"job_id": state.job_id, "job_status": state.job_status}

