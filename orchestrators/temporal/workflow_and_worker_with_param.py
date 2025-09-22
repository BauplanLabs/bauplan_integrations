# workflow_and_worker_with_param.py
import asyncio
from datetime import timedelta
from uuid import uuid4
from typing import Dict, Any

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker
from concurrent.futures import ThreadPoolExecutor

# Import activity via passthrough so the workflow sandbox doesn’t re-import bauplan
with workflow.unsafe.imports_passed_through():
    from activities import run_with_parameters_activity


@workflow.defn
class RunWithParametersWorkflow:
    @workflow.run
    async def run(
        self,
        project_dir: str,
        bauplan_branch_suffix: str,
        parameters: Dict[str, Any],
    ) -> dict:
        return await workflow.execute_activity(
            run_with_parameters_activity,
            args=[project_dir, bauplan_branch_suffix, parameters],
            schedule_to_close_timeout=timedelta(minutes=60),
        )


async def main():
    client = await Client.connect("localhost:7233")
    task_queue = "bauplan-parameters"

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[RunWithParametersWorkflow],
        activities=[run_with_parameters_activity],
        activity_executor=ThreadPoolExecutor(max_workers=8),  # needed since activity is sync
    )

    async with worker:
        handle = await client.start_workflow(
            RunWithParametersWorkflow.run,
            id=f"bauplan-params-{uuid4()}",
            task_queue=task_queue,
            args=[
                "your_bauplan_project",  # change this with the path to your bauplan project
                "temporal_docs",           # change this with your branch suffix
                {"start_time": "2023-01-01T00:00:00-05:00"},
            ],
        )
        result = await handle.result()
        print("Workflow result:", result)


if __name__ == "__main__":
    asyncio.run(main())