from prefect import flow, task, get_run_logger
import bauplan


@task(name="run-bauplan-pipeline")
def run_pipeline(
        project_dir: str,
        bauplan_branch_suffix: str,
) -> dict:
    """
    Execute a Bauplan project and return a minimal, serializable summary.

    Returns:
        dict with 'job_id' and 'job_status'.
    Raises:
        RuntimeError if the job did not finish with status 'success'.
    """
    logger = get_run_logger()
    client = bauplan.Client()  # build client inside the task

    username = client.info().user.username  # get username
    branch = f"{username}.{bauplan_branch_suffix}"  # construct branch name

    state = client.run(project_dir=project_dir, ref=branch)

    status = (state.job_status or "").lower()
    logger.info(f"Bauplan job {state.job_id} finished with status='{status}'")

    if status != "success":
        raise RuntimeError(f"Bauplan job {state.job_id} ended with status='{status}'")

    # return only simple types so Prefect can store results safely
    return {"job_id": state.job_id, "job_status": status}


@flow(name="bauplan-pipeline-run")
def main(
        pipeline_dir: str,
        bauplan_branch_suffix: str,
) -> dict:
    """
    Minimal Prefect → Bauplan integration: run a project and fail fast on error.
    """

    result = run_pipeline(project_dir=pipeline_dir, bauplan_branch_suffix=bauplan_branch_suffix)

    get_run_logger().info(
        f"Bauplan run succeeded (job_id={result['job_id']}, status={result['job_status']})"
    )
    return result


if __name__ == "__main__":
    pipeline_path = '../../bauplan_pipelines'
    branch_suffix = 'prefect_docs'
    main(pipeline_path, branch_suffix)

