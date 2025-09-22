# bauplan_prefect_with_params.py
from prefect import flow
import bauplan


@flow
def run_with_parameters(
        project_dir: str,
        bauplan_branch_suffix: str,  # your own branch
        parameters: dict # your arbitrary parameters
    ):

    client = bauplan.Client()  # build client inside the task

    username = client.info().user.username  # get username
    branch = f"{username}.{bauplan_branch_suffix}"  # construct branch name

    state = client.run(project_dir=project_dir, ref=branch, parameters=parameters)

    if state.job_status != "SUCCESS":
        raise RuntimeError(f"Bauplan job {state.job_id} ended with status='{state.job_status}'")

    # return only simple types so Prefect can store results safely
    return {"job_id": state.job_id, "job_status": state.job_status}


if __name__ == "__main__":
    pipeline_path = 'your_bauplan_project' # change this with the path to your bauplan project
    branch_suffix = 'prefect_docs' # change this with the name of your branch
    run_with_parameters(
        project_dir=pipeline_path,
        bauplan_branch_suffix=branch_suffix,
        parameters={"start_time": "2023-01-01T00:00:00-05:00"}
    )