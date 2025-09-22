import asyncio
from datetime import timedelta
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

# Import activities as pass-through so the workflow sandbox doesn't re-import bauplan
with workflow.unsafe.imports_passed_through():
    from activities import run_bauplan_activity, report_success_activity


@workflow.defn
class BauplanWorkflow:
    @workflow.run
    async def run(self, pipeline_dir: str, bauplan_branch_suffix: str) -> dict:
        result = await workflow.execute_activity(
            run_bauplan_activity,
            args=[pipeline_dir, bauplan_branch_suffix],
            schedule_to_close_timeout=timedelta(minutes=60),
        )
        await workflow.execute_activity(
            report_success_activity,
            args=[result],
            schedule_to_close_timeout=timedelta(seconds=30),
        )
        return result


async def main():
    client = await Client.connect("localhost:7233")
    task_queue = "bauplan-pipeline-run"

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[BauplanWorkflow],
        activities=[run_bauplan_activity, report_success_activity],
        # Needed because activities are sync (blocking I/O)
        activity_executor=ThreadPoolExecutor(max_workers=8),
    )

    async with worker:
        handle = await client.start_workflow(
            BauplanWorkflow.run,
            id=f"bauplan-{uuid4()}",
            task_queue=task_queue,
            args=[
                "/Users/cirogreco/PycharmProjects/bauplan_sketchbook/bauplan_pipelines",  # path to your Bauplan project folder
                "temporal_docs",         # your branch suffix
            ],
        )
        print("Workflow result:", await handle.result())


if __name__ == "__main__":
    asyncio.run(main())
