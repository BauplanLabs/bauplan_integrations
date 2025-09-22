# bauplan_airflow3_with_params.py
from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
import pendulum
import bauplan

@task
def run_with_parameters_task(
    project_dir: str,
    bauplan_branch_suffix: str,
    parameters: dict,
) -> dict:
    client = bauplan.Client()  # build client inside the task

    username = client.info().user.username
    branch = f"{username}.{bauplan_branch_suffix}"

    state = client.run(project_dir=project_dir, ref=branch, parameters=parameters)

    if str(state.job_status).lower() != "success":
        raise AirflowException(
            f"Bauplan job {state.job_id} ended with status='{state.job_status}'"
        )

    return {"job_id": state.job_id, "job_status": state.job_status}


@dag(
    dag_id="bauplan-pipeline-run-with-parameters",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bauplan", "parameters"],
)
def run_with_parameters(
    project_dir: str,
    bauplan_branch_suffix: str,  # your own branch
    parameters: dict,            # your arbitrary parameters
):
    run_with_parameters_task(
        project_dir=project_dir,
        bauplan_branch_suffix=bauplan_branch_suffix,
        parameters=parameters,
    )

dag = run_with_parameters(
    project_dir="your_bauplan_project", # change this with the path to your bauplan project
    bauplan_branch_suffix="your_branch", # change this with the name of your branch
    parameters={"start_time": "2023-01-01T00:00:00-05:00"},
)

if __name__ == "__main__":
    dag.test()