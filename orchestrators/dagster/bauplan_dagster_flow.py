# bauplan_dagster_flow.py
import dagster as dg
import bauplan

@dg.op(
    name="run_bauplan_pipeline",
    config_schema={"project_dir": str, "bauplan_branch_suffix": str},
)
def run_pipeline(context) -> dict:
    """
    Execute a Bauplan project and return a minimal, serializable summary.

    Returns:
        dict with 'job_id' and 'job_status'.
    Raises:
        dg.Failure if the job did not finish with status 'success'.
    """
    client = bauplan.Client()  # build client inside the op

    username = client.info().user.username  # get username
    cfg = context.op_config
    branch = f"{username}.{cfg['bauplan_branch_suffix']}"  # construct branch name

    state = client.run(project_dir=cfg["project_dir"], ref=branch)

    if str(state.job_status).lower() != "success":
        raise dg.Failure(
            description=f"Bauplan job {state.job_id} ended with status='{state.job_status}'"
        )

    # return only simple types so Dagster can store results safely
    return {"job_id": state.job_id, "job_status": state.job_status}

@dg.op(name="report_success")
def report_success(context, result: dict) -> None:
    context.log.info(
        f"Bauplan run succeeded (job_id={result['job_id']}, status={result['job_status']})"
    )

@dg.job(name="bauplan_pipeline_run")
def main_job():
    report_success(run_pipeline())

# Optional: make jobs discoverable by `dagster dev -m bauplan_dagster_job`
defs = dg.Definitions(jobs=[main_job])

if __name__ == "__main__":
    # Fast local debugging: run in a single process
    result = main_job.execute_in_process(
        run_config={
            "ops": {
                "run_bauplan_pipeline": {
                    "config": {
                        "project_dir": "/Users/cirogreco/PycharmProjects/bauplan_sketchbook/bauplan_pipelines",
                        "bauplan_branch_suffix": "dagster_docs",
                    }
                }
            }
        }
    )
    assert result.success


