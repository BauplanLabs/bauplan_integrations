# bauplan_dagster_with_params.py
import dagster as dg
import bauplan

@dg.op(
    name="run_bauplan_with_parameters",
    config_schema={
        "project_dir": str,
        "bauplan_branch_suffix": str,
        "parameters": dict,   # arbitrary parameters for your Bauplan project
    },
)
def run_with_parameters(context) -> dict:
    client = bauplan.Client()  # build client inside the op

    username = client.info().user.username
    cfg = context.op_config
    branch = f"{username}.{cfg['bauplan_branch_suffix']}"

    state = client.run(
        project_dir=cfg["project_dir"],
        ref=branch,
        parameters=cfg["parameters"],
    )

    if str(state.job_status).lower() != "success":
        raise dg.Failure(
            description=f"Bauplan job {state.job_id} ended with status='{state.job_status}'"
        )

    return {"job_id": state.job_id, "job_status": state.job_status}

@dg.job(name="bauplan_pipeline_run_with_parameters")
def run_with_parameters_job():
    run_with_parameters()

defs = dg.Definitions(jobs=[run_with_parameters_job])

if __name__ == "__main__":
    run_config = {
        "ops": {
            "run_bauplan_with_parameters": {
                "config": {
                    "project_dir": "/Users/cirogreco/PycharmProjects/bauplan_sketchbook/model_param",
                    "bauplan_branch_suffix": "dagster_docs",
                    "parameters": {"start_time": "2023-01-01T00:00:00-05:00"},
                }
            }
        }
    }
    result = run_with_parameters_job.execute_in_process(run_config=run_config)
    assert result.success


