# bauplan_airflow3_flow.py
from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
import pendulum
import bauplan


@task(task_id="run-bauplan-pipeline")
def run_pipeline(
    project_dir: str,
    bauplan_branch_suffix: str,
) -> dict:
    """
    Execute a Bauplan project and return a minimal, serializable summary.

    Returns:
        dict with 'job_id' and 'job_status'.
    Raises:
        AirflowException if the job did not finish with status 'success'.
    """
    client = bauplan.Client()  # build client inside the task

    username = client.info().user.username  # get username
    branch = f"{username}.{bauplan_branch_suffix}"  # construct branch name

    state = client.run(project_dir=project_dir, ref=branch)

    if str(state.job_status).lower() != "success":
        raise AirflowException(
            f"Bauplan job {state.job_id} ended with status='{state.job_status}'"
        )

    # return only simple types so Airflow can store results safely via XCom
    return {"job_id": state.job_id, "job_status": state.job_status}


@task(task_id="report-success")
def report_success(result: dict) -> None:
    print(f"Bauplan run succeeded (job_id={result['job_id']}, status={result['job_status']})")


@dag(
    dag_id="bauplan-pipeline-run",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bauplan", "integration"],
)
def main(
    pipeline_dir: str,
    bauplan_branch_suffix: str,
):
    """
    Minimal Airflow → Bauplan integration: run a project and fail fast on error.
    """
    result = run_pipeline(project_dir=pipeline_dir, bauplan_branch_suffix=bauplan_branch_suffix)
    report_success(result)


# Instantiate the DAG with your arguments
dag = main(
    pipeline_dir="your_bauplan_project", # change this with the path to your bauplan project
    bauplan_branch_suffix="airflow_docs", # change this with the name of your branch
)

if __name__ == "__main__":
    dag.test()