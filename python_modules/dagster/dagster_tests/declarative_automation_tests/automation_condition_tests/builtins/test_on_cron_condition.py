import datetime
import time
from collections.abc import Set

import dagster as dg
import pytest
from dagster import AutomationCondition, DagsterInstance, Definitions
from dagster._time import datetime_from_timestamp
from dagster_shared.check.functions import ParameterCheckError

from dagster_tests.declarative_automation_tests.scenario_utils.automation_condition_scenario import (
    AutomationConditionScenarioState,
)
from dagster_tests.declarative_automation_tests.scenario_utils.base_scenario import run_request
from dagster_tests.declarative_automation_tests.scenario_utils.scenario_specs import (
    hourly_partitions_def,
    two_assets_in_sequence,
)


@pytest.mark.asyncio
async def test_on_cron_unpartitioned() -> None:
    state = AutomationConditionScenarioState(
        two_assets_in_sequence,
        automation_condition=AutomationCondition.on_cron(cron_schedule="0 * * * *"),
        ensure_empty_result=False,
    ).with_current_time("2020-02-02T00:55:00")

    # no cron boundary crossed
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # now crossed a cron boundary parent hasn't updated yet
    state = state.with_current_time_advanced(minutes=10)
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # parent updated, now can execute
    state = state.with_runs(run_request("A"))
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 1
    state = state.with_runs(
        *(
            run_request(ak, pk)
            for ak, pk in result.true_subset.expensively_compute_asset_partitions()
        )
    )

    # now B has been materialized, so don't execute again
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # A gets materialized again before the hour, so don't execute B again
    state = state.with_runs(run_request("A"))
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # now a new cron tick, but A still hasn't been materialized since the hour
    state = state.with_current_time_advanced(hours=1)
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # A gets materialized again after the hour, so execute B again
    state = state.with_runs(run_request("A"))
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 1


@pytest.mark.asyncio
async def test_on_cron_hourly_partitioned() -> None:
    state = (
        AutomationConditionScenarioState(
            two_assets_in_sequence,
            automation_condition=AutomationCondition.on_cron(cron_schedule="0 * * * *"),
            ensure_empty_result=False,
        )
        .with_asset_properties(partitions_def=hourly_partitions_def)
        .with_current_time("2020-02-02T00:55:00")
    )

    # no cron boundary crossed
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # now crossed a cron boundary parent hasn't updated yet
    state = state.with_current_time_advanced(minutes=10)
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # historical parent updated, doesn't matter
    state = state.with_runs(run_request("A", "2019-07-05-00:00"))
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # latest parent updated, now can execute
    state = state.with_runs(run_request("A", "2020-02-02-00:00"))
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 1
    state = state.with_runs(
        *(
            run_request(ak, pk)
            for ak, pk in result.true_subset.expensively_compute_asset_partitions()
        )
    )

    # now B has been materialized, so don't execute again
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # now a new cron tick, but A still hasn't been materialized since the hour
    state = state.with_current_time_advanced(hours=1)
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # A gets materialized with the previous partition after the hour, but that doesn't matter
    state = state.with_runs(run_request("A", "2020-02-02-00:00"))
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 0

    # A gets materialized with the latest partition, fire
    state = state.with_runs(run_request("A", "2020-02-02-01:00"))
    state, result = await state.evaluate("B")
    assert result.true_subset.size == 1


def test_on_cron_on_asset_check() -> None:
    @dg.asset
    def A() -> None: ...

    @dg.asset_check(asset=A, automation_condition=AutomationCondition.on_cron("@hourly"))
    def foo_check() -> ...: ...

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = dg.Definitions(assets=[A], asset_checks=[foo_check])
    instance = DagsterInstance.ephemeral()

    # hasn't passed a cron tick
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # now passed a cron tick, but parent hasn't been updated
    current_time += datetime.timedelta(minutes=30)
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # now parent is updated, so fire
    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(dg.AssetMaterialization("A"))
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1

    # don't keep firing
    current_time += datetime.timedelta(minutes=1)
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # ...even if the parent is updated again
    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(dg.AssetMaterialization("A"))
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # new tick passes...
    current_time += datetime.timedelta(hours=1)
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # and parent is updated
    current_time += datetime.timedelta(minutes=1)
    instance.report_runless_asset_event(dg.AssetMaterialization("A"))
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 1


def test_on_cron_on_observable_source() -> None:
    @dg.observable_source_asset(automation_condition=AutomationCondition.on_cron("@hourly"))
    def obs() -> None: ...

    @dg.asset(deps=[obs], automation_condition=AutomationCondition.on_cron("@hourly"))
    def mat() -> None: ...

    current_time = datetime.datetime(2024, 8, 16, 4, 35)
    defs = dg.Definitions(assets=[obs, mat])
    instance = DagsterInstance.ephemeral()

    # hasn't passed a cron tick
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # now passed a cron tick, kick off both
    current_time += datetime.timedelta(minutes=30)
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 2

    # don't kick off again
    current_time += datetime.timedelta(minutes=1)
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 0

    # next hour, kick off again
    current_time += datetime.timedelta(hours=1)
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result.cursor, evaluation_time=current_time
    )
    assert result.total_requested == 2


def test_asset_order_change_doesnt_reset_cursor_state() -> None:
    @dg.asset
    def A() -> None: ...

    @dg.asset
    def B() -> None: ...

    @dg.asset
    def C() -> None: ...

    @dg.asset(
        key=dg.AssetKey(["downstream"]),
        deps=[B, C],
        automation_condition=AutomationCondition.all_deps_updated_since_cron("* * * * *", "UTC"),
    )
    def downstream_before() -> None: ...

    @dg.asset(
        key=dg.AssetKey(["downstream"]),
        deps=[A, B, C],
        automation_condition=AutomationCondition.all_deps_updated_since_cron("* * * * *", "UTC"),
    )
    def downstream_aftter() -> None: ...

    # B is at index 0 before
    defs_before = dg.Definitions(assets=[B, C, downstream_before])

    # B is at index 1 after
    defs_after = dg.Definitions(assets=[A, B, C, downstream_aftter])

    instance = DagsterInstance.ephemeral()

    def _emit_check(defs: Definitions, checks: Set[dg.AssetCheckKey], passed: bool):
        defs.resolve_implicit_global_asset_job_def().get_subset(
            asset_check_selection=checks
        ).execute_in_process(
            tags={"passed": ""} if passed else None, instance=instance, raise_on_error=passed
        )

    start_time = time.time()

    result = dg.evaluate_automation_conditions(
        defs=defs_before, instance=instance, evaluation_time=datetime_from_timestamp(start_time)
    )
    assert result.total_requested == 0

    # Cross a cron boundary
    start_time += 60
    result = dg.evaluate_automation_conditions(
        defs=defs_before, instance=instance, evaluation_time=datetime_from_timestamp(start_time)
    )
    assert result.total_requested == 0

    dg.materialize([B], instance=instance)

    result = dg.evaluate_automation_conditions(
        defs=defs_before,
        instance=instance,
        cursor=result.cursor,
        evaluation_time=datetime_from_timestamp(start_time),
    )
    assert result.total_requested == 0

    dg.materialize([A], instance=instance)
    dg.materialize([C], instance=instance)

    result = dg.evaluate_automation_conditions(
        defs=defs_after,
        instance=instance,
        cursor=result.cursor,
        evaluation_time=datetime_from_timestamp(start_time),
    )
    assert result.total_requested == 1


@pytest.mark.parametrize(
    "schedule, should_fail",
    [
        ("0 0 * * *", False),
        ("0 */5 * * *", False),
        ("@daily", False),
        ("@hourly", False),
        ("*/15 * * * 1-6", False),
        ("* /15 * * * 1-6", True),
        ("totally-invalid", True),
    ],
)
def test_invalid_schedules(schedule: str, should_fail: bool) -> None:
    if should_fail:
        with pytest.raises(
            ParameterCheckError,
            match="Invalid cron schedule",
        ):
            AutomationCondition.on_cron(cron_schedule=schedule)
    else:
        AutomationCondition.on_cron(cron_schedule=schedule)
