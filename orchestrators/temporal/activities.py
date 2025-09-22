from temporalio import activity

@activity.defn
def run_bauplan_activity(project_dir: str, bauplan_branch_suffix: str) -> dict:
    # Import inside the activity to avoid the workflow sandbox
    import bauplan

    client = bauplan.Client()
    username = client.info().user.username
    branch = f"{username}.{bauplan_branch_suffix}"

    state = client.run(project_dir=project_dir, ref=branch)
    if str(state.job_status).lower() != "success":
        raise RuntimeError(
            f"Bauplan job {state.job_id} ended with status='{state.job_status}'"
        )
    return {"job_id": state.job_id, "job_status": state.job_status}

@activity.defn
def report_success_activity(result: dict) -> None:
    print(f"Bauplan run succeeded (job_id={result['job_id']}, status={result['job_status']})")
