import re

import pytest

from dagster import execute_job, ReexecutionOptions, reconstructable
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.pipeline_base import InMemoryPipeline
from dagster._core.errors import (
    DagsterExecutionStepNotFoundError,
    DagsterInvalidSubsetError,
)
from dagster._core.execution.api import create_execution_plan, execute_run
from dagster._core.instance import DagsterInstance
from dagster._core.test_utils import instance_for_test, step_output_event_filter
from dagster._legacy import (
    reexecute_pipeline,
)

from .test_subset_selector import asset_selection_job, foo_job


def execute_ijob_for_test(i_job, instance, asset_selection=None):
    execution_plan = create_execution_plan(i_job)
    run = instance.create_run_for_pipeline(
        i_job.get_definition(),
        asset_selection=asset_selection,
        execution_plan=execution_plan,
    )

    return execute_run(i_job, run, instance)


def test_subset_for_execution():
    pipeline = InMemoryPipeline(foo_job)
    sub_pipeline = pipeline.subset_for_execution(["*add_nums"], asset_selection=None)

    assert sub_pipeline.solid_selection == ["*add_nums"]
    assert sub_pipeline.solids_to_execute == {"add_nums", "return_one", "return_two"}

    with instance_for_test() as instance:
        result = execute_ijob_for_test(sub_pipeline, instance)
        assert result.success


def test_asset_subset_for_execution():
    in_mem_pipeline = InMemoryPipeline(asset_selection_job)
    sub_pipeline = in_mem_pipeline.subset_for_execution(
        solid_selection=None, asset_selection={AssetKey("my_asset")}
    )
    assert sub_pipeline.asset_selection == {AssetKey("my_asset")}

    with instance_for_test() as instance:
        result = execute_ijob_for_test(
            sub_pipeline, instance, asset_selection={AssetKey("my_asset")}
        )
        assert result.success

        materializations = [
            event for event in result.step_event_list if event.is_step_materialization
        ]
        assert len(materializations) == 1
        assert materializations[0].asset_key == AssetKey("my_asset")


def test_reexecute_asset_subset():
    with instance_for_test() as instance:
        result = asset_selection_job.execute_in_process(
            instance=instance, asset_selection=[AssetKey("my_asset")]
        )
        assert result.success
        materializations = [event for event in result.all_events if event.is_step_materialization]
        assert len(materializations) == 1
        assert materializations[0].asset_key == AssetKey("my_asset")

        run = instance.get_run_by_id(result.run_id)
        assert run.asset_selection == {AssetKey("my_asset")}

        reexecution_result = reexecute_pipeline(
            asset_selection_job,
            parent_run_id=result.run_id,
            instance=instance,
        )
        assert reexecution_result.success
        materializations = [
            event for event in reexecution_result.step_event_list if event.is_step_materialization
        ]
        assert len(materializations) == 1
        assert materializations[0].asset_key == AssetKey("my_asset")
        run = instance.get_run_by_id(reexecution_result.run_id)
        assert run.asset_selection == {AssetKey("my_asset")}


def test_execute_pipeline_with_solid_selection_single_clause():
    pipeline_result_full = foo_job.execute_in_process()
    assert pipeline_result_full.success
    assert pipeline_result_full.output_for_node("add_one") == 7
    assert len(pipeline_result_full.get_step_success_events()) == 5

    pipeline_result_up = foo_job.execute_in_process(op_selection=["*add_nums"])
    assert pipeline_result_up.success
    assert pipeline_result_up.output_for_node("add_nums") == 3
    assert len(pipeline_result_up.get_step_success_events()) == 3

    job_result_down = foo_job.execute_in_process(
        run_config={
            "solids": {"add_nums": {"inputs": {"num1": {"value": 1}, "num2": {"value": 2}}}}
        },
        op_selection=["add_nums++"],
    )
    assert job_result_down.success
    assert job_result_down.output_for_node("add_one") == 7
    assert len(job_result_down.get_step_success_events()) == 3


def test_execute_pipeline_with_solid_selection_multi_clauses():
    result_multi_disjoint = foo_job.execute_in_process(
        op_selection=["return_one", "return_two", "add_nums+"]
    )
    assert result_multi_disjoint.success
    assert result_multi_disjoint.output_for_node("multiply_two") == 6
    assert len(result_multi_disjoint.get_step_success_events()) == 4

    result_multi_overlap = foo_job.execute_in_process(
        op_selection=["return_one++", "add_nums+", "return_two"]
    )
    assert result_multi_overlap.success
    assert result_multi_overlap.output_for_node("multiply_two") == 6
    assert len(result_multi_overlap.get_step_success_events()) == 4

    with pytest.raises(
        DagsterInvalidSubsetError,
        match=re.escape("No qualified ops to execute found for op_selection"),
    ):
        foo_job.execute_in_process(op_selection=["a", "*add_nums"])


def test_execute_pipeline_with_solid_selection_invalid():
    invalid_input = ["return_one,return_two"]

    with pytest.raises(
        DagsterInvalidSubsetError,
        match=re.escape(
            "No qualified ops to execute found for op_selection={input}".format(input=invalid_input)
        ),
    ):
        foo_job.execute_in_process(op_selection=invalid_input)


def test_reexecute_pipeline_with_step_selection_single_clause():
    with instance_for_test() as instance:
        pipeline_result_full = foo_job.execute_in_process(instance=instance)
        assert pipeline_result_full.success
        assert pipeline_result_full.output_for_node("add_one") == 7
        assert len(pipeline_result_full.get_step_success_events()) == 5

        with execute_job(
            reconstructable(foo_job),
            instance=instance,
            reexecution_options=ReexecutionOptions(parent_run_id=pipeline_result_full.run_id),
        ) as reexecution_result_full:

            assert reexecution_result_full.success
            assert len(reexecution_result_full.get_step_success_events()) == 5
            assert reexecution_result_full.output_for_node("add_one") == 7

        with execute_job(
            reconstructable(foo_job),
            instance=instance,
            reexecution_options=ReexecutionOptions(
                parent_run_id=pipeline_result_full.run_id,
                step_selection=["*add_nums"],
            ),
        ) as reexecution_result_up:

            assert reexecution_result_up.success
            assert reexecution_result_up.output_for_node("add_nums") == 3

        with execute_job(
            reconstructable(foo_job),
            instance=instance,
            reexecution_options=ReexecutionOptions(
                parent_run_id=pipeline_result_full.run_id,
                step_selection=["add_nums++"],
            ),
            raise_on_error=True,
        ) as reexecution_result_down:
            assert reexecution_result_down.success
            assert reexecution_result_down.output_for_node("add_one") == 7


def test_reexecute_pipeline_with_step_selection_multi_clauses():
    instance = DagsterInstance.ephemeral()
    pipeline_result_full = foo_job.execute_in_process(instance=instance)
    assert pipeline_result_full.success
    assert pipeline_result_full.output_for_node("add_one") == 7
    assert len(pipeline_result_full.get_step_success_events()) == 5

    result_multi_disjoint = execute_job(
        reconstructable(foo_job),
        instance=instance,
        reexecution_options=ReexecutionOptions(
            parent_run_id=pipeline_result_full.run_id,
            step_selection=["return_one", "return_two", "add_nums+"],
        ),
    )
    assert result_multi_disjoint.success
    assert result_multi_disjoint.output_for_node("multiply_two") == 6

    result_multi_overlap = execute_job(
        reconstructable(foo_job),
        instance=instance,
        reexecution_options=ReexecutionOptions(
            parent_run_id=pipeline_result_full.run_id,
            step_selection=["return_one++", "return_two", "add_nums+"],
        ),
    )
    assert result_multi_overlap.success
    assert result_multi_overlap.output_for_node("multiply_two") == 6

    with pytest.raises(
        DagsterExecutionStepNotFoundError,
        match="Step selection refers to unknown step: a",
    ):
        execute_job(
            reconstructable(foo_job),
            instance=instance,
            reexecution_options=ReexecutionOptions(
                parent_run_id=pipeline_result_full.run_id,
                step_selection=["a", "*add_nums"],
            ),
        )

    with pytest.raises(
        DagsterExecutionStepNotFoundError,
        match="Step selection refers to unknown steps: a, b",
    ):
        execute_job(
            reconstructable(foo_job),
            instance=instance,
            reexecution_options=ReexecutionOptions(
                parent_run_id=pipeline_result_full.run_id,
                step_selection=["a+", "*b"],
            ),
        )
