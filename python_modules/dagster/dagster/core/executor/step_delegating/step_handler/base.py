from abc import ABC, abstractmethod
from typing import Dict, Iterator, List, NamedTuple, Optional

from dagster import DagsterInstance
from dagster import _check as check
from dagster.core.events import DagsterEvent
from dagster.core.execution.context.system import IStepContext, PlanOrchestrationContext
from dagster.core.execution.plan.step import ExecutionStep
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.grpc.types import ExecuteStepArgs


class StepHandlerContext:
    def __init__(
        self,
        instance: DagsterInstance,
        plan_context: PlanOrchestrationContext,
        steps: List[ExecutionStep],
        execute_step_args: ExecuteStepArgs,
        pipeline_run: Optional[PipelineRun] = None,
    ) -> None:
        self._instance = instance
        self._plan_context = plan_context
        self._steps_by_key = {step.key: step for step in steps}
        self._execute_step_args = execute_step_args
        self._pipeline_run = pipeline_run

    @property
    def execute_step_args(self) -> ExecuteStepArgs:
        return self._execute_step_args

    @property
    def pipeline_run(self) -> PipelineRun:
        # lazy load
        if not self._pipeline_run:
            run_id = self.execute_step_args.pipeline_run_id
            run = self._instance.get_run_by_id(run_id)
            if run is None:
                check.failed(f"Failed to load run {run_id}")
            self._pipeline_run = run

        return self._pipeline_run

    @property
    def step_tags(self) -> Dict[str, Dict[str, str]]:
        return {step_key: step.tags for step_key, step in self._steps_by_key.items()}

    @property
    def instance(self) -> DagsterInstance:
        return self._instance

    def get_step_context(self, step_key: str) -> IStepContext:
        return self._plan_context.for_step(self._steps_by_key[step_key])


class CheckStepHealthResult(
    NamedTuple(
        "_CheckStepHealthResult", [("is_healthy", bool), ("unhealthy_reason", Optional[str])]
    )
):
    def __new__(cls, is_healthy: bool, unhealthy_reason: Optional[str] = None):
        return super(CheckStepHealthResult, cls).__new__(
            cls,
            check.bool_param(is_healthy, "is_healthy"),
            check.opt_str_param(unhealthy_reason, "unhealthy_reason"),
        )

    @staticmethod
    def healthy() -> "CheckStepHealthResult":
        return CheckStepHealthResult(is_healthy=True)

    @staticmethod
    def unhealthy(reason: str) -> "CheckStepHealthResult":
        return CheckStepHealthResult(is_healthy=False, unhealthy_reason=reason)


class StepHandler(ABC):  # pylint: disable=no-init
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def launch_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        pass

    @abstractmethod
    def check_step_health(self, step_handler_context: StepHandlerContext) -> CheckStepHealthResult:
        pass

    @abstractmethod
    def terminate_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        pass
