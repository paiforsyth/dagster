import subprocess
import time

from dagster import (
    executor,
    job,
    op,
    reconstructable,
    execute_job,
    ReexecutionOptions,
    DynamicOut,
    DynamicOutput,
    resource,
)
import re
from dagster._config import Permissive
from dagster._core.definitions.executor_definition import multiple_process_executor_requirements
from dagster._core.events import DagsterEventType
from dagster._core.execution.api import execute_pipeline
from dagster._core.execution.retries import RetryMode
from dagster._core.executor.step_delegating import (
    CheckStepHealthResult,
    StepDelegatingExecutor,
    StepHandler,
)
from dagster._core.test_utils import instance_for_test
from dagster._utils import merge_dicts
import pickle
import os
from tempfile import TemporaryDirectory


class TestStepHandler(StepHandler):
    # This step handler waits for all processes to exit, because windows tests flake when processes
    # are left alive when the test ends. Non-test step handlers should not keep their own state in memory.
    processes = []  # type: ignore
    launch_step_count = 0  # type: ignore
    saw_baz_op = False
    check_step_health_count = 0  # type: ignore
    terminate_step_count = 0  # type: ignore
    verify_step_count = 0  # type: ignore

    @property
    def name(self):
        return "TestStepHandler"

    def launch_step(self, step_handler_context):
        if step_handler_context.execute_step_args.should_verify_step:
            TestStepHandler.verify_step_count += 1
        if step_handler_context.execute_step_args.step_keys_to_execute[0] == "baz_op":
            TestStepHandler.saw_baz_op = True
            assert step_handler_context.step_tags["baz_op"] == {"foo": "bar"}

        TestStepHandler.launch_step_count += 1
        print("TestStepHandler Launching Step!")  # pylint: disable=print-call
        TestStepHandler.processes.append(
            subprocess.Popen(step_handler_context.execute_step_args.get_command_args())
        )
        return iter(())

    def check_step_health(self, step_handler_context) -> CheckStepHealthResult:
        TestStepHandler.check_step_health_count += 1
        return CheckStepHealthResult.healthy()

    def terminate_step(self, step_handler_context):
        TestStepHandler.terminate_step_count += 1
        raise NotImplementedError()

    @classmethod
    def reset(cls):
        cls.processes = []
        cls.launch_step_count = 0
        cls.check_step_health_count = 0
        cls.terminate_step_count = 0
        cls.verify_step_count = 0

    @classmethod
    def wait_for_processes(cls):
        for p in cls.processes:
            p.wait(timeout=5)


@executor(
    name="test_step_delegating_executor",
    requirements=multiple_process_executor_requirements(),
    config_schema=Permissive(),
)
def test_step_delegating_executor(exc_init):
    return StepDelegatingExecutor(
        TestStepHandler(),
        **(merge_dicts({"retries": RetryMode.DISABLED}, exc_init.executor_config)),
    )


# @op
# def bar_op(_):
#     return "bar"


# @op(tags={"foo": "bar"})
# def baz_op(_, bar):
#     return bar * 2


# @job(executor_def=test_step_delegating_executor)
# def foo_job():
#     baz_op(bar_op())
#     bar_op()


# def test_execute():
#     TestStepHandler.reset()
#     with instance_for_test() as instance:
#         result = execute_pipeline(
#             reconstructable(foo_job),
#             instance=instance,
#             run_config={"execution": {"config": {}}},
#         )
#         TestStepHandler.wait_for_processes()

#     assert any(
#         [
#             "Starting execution with step handler TestStepHandler" in event.message
#             for event in result.event_list
#         ]
#     )
#     assert any(["STEP_START" in event for event in result.event_list])
#     assert result.success
#     assert TestStepHandler.saw_baz_op
#     assert TestStepHandler.verify_step_count == 0


# def test_skip_execute():
#     from .test_jobs import define_dynamic_skipping_job

#     TestStepHandler.reset()
#     with instance_for_test() as instance:
#         result = execute_pipeline(
#             reconstructable(define_dynamic_skipping_job),
#             instance=instance,
#         )
#         TestStepHandler.wait_for_processes()

#     assert result.success


# def test_dynamic_execute():
#     from .test_jobs import define_dynamic_job

#     TestStepHandler.reset()
#     with instance_for_test() as instance:
#         result = execute_pipeline(
#             reconstructable(define_dynamic_job),
#             instance=instance,
#         )
#         TestStepHandler.wait_for_processes()

#     assert result.success
#     assert (
#         len(
#             [
#                 e
#                 for e in result.event_list
#                 if e.event_type_value == DagsterEventType.STEP_START.value
#             ]
#         )
#         == 11
#     )


# def test_skipping():
#     from .test_jobs import define_skpping_job

#     TestStepHandler.reset()
#     with instance_for_test() as instance:
#         result = execute_pipeline(
#             reconstructable(define_skpping_job),
#             instance=instance,
#         )
#         TestStepHandler.wait_for_processes()

#     assert result.success


# def test_execute_intervals():
#     TestStepHandler.reset()
#     with instance_for_test() as instance:
#         result = execute_pipeline(
#             reconstructable(foo_job),
#             instance=instance,
#             run_config={"execution": {"config": {"check_step_health_interval_seconds": 60}}},
#         )
#         TestStepHandler.wait_for_processes()

#     assert result.success
#     assert TestStepHandler.launch_step_count == 3
#     assert TestStepHandler.terminate_step_count == 0
#     # pipeline should complete before 60s
#     assert TestStepHandler.check_step_health_count == 0

#     TestStepHandler.reset()
#     with instance_for_test() as instance:
#         result = execute_pipeline(
#             reconstructable(foo_job),
#             instance=instance,
#             run_config={"execution": {"config": {"check_step_health_interval_seconds": 0}}},
#         )
#         TestStepHandler.wait_for_processes()

#     assert result.success
#     assert TestStepHandler.launch_step_count == 3
#     assert TestStepHandler.terminate_step_count == 0
#     # every step should get checked at least once
#     assert TestStepHandler.check_step_health_count >= 3


# @op
# def slow_op(_):
#     time.sleep(2)


# @job(executor_def=test_step_delegating_executor)
# def three_op_job():
#     for i in range(3):
#         slow_op.alias(f"slow_op_{i}")()


# def test_max_concurrent():
#     TestStepHandler.reset()
#     with instance_for_test() as instance:
#         result = execute_pipeline(
#             reconstructable(three_op_job),
#             instance=instance,
#             run_config={"execution": {"config": {"max_concurrent": 1}}},
#         )
#         TestStepHandler.wait_for_processes()
#     assert result.success

#     # test that all the steps run serially, since max_concurrent is 1
#     active_step = None
#     for event in result.event_list:
#         if event.event_type_value == DagsterEventType.STEP_START.value:
#             assert active_step is None, "A second step started before the first finished!"
#             active_step = event.step_key
#         elif event.event_type_value == DagsterEventType.STEP_SUCCESS.value:
#             assert (
#                 active_step == event.step_key
#             ), "A step finished that wasn't supposed to be active!"
#             active_step = None


# @executor(
#     name="test_step_delegating_executor_verify_step",
#     requirements=multiple_process_executor_requirements(),
#     config_schema=Permissive(),
# )
# def test_step_delegating_executor_verify_step(exc_init):
#     return StepDelegatingExecutor(
#         TestStepHandler(),
#         retries=RetryMode.DISABLED,
#         sleep_seconds=exc_init.executor_config.get("sleep_seconds"),
#         check_step_health_interval_seconds=exc_init.executor_config.get(
#             "check_step_health_interval_seconds"
#         ),
#         should_verify_step=True,
#     )


# @job(executor_def=test_step_delegating_executor_verify_step)
# def foo_job_verify_step():
#     baz_op(bar_op())
#     bar_op()


# def test_execute_verify_step():
#     TestStepHandler.reset()
#     with instance_for_test() as instance:
#         result = execute_pipeline(
#             reconstructable(foo_job_verify_step),
#             instance=instance,
#             run_config={"execution": {"config": {}}},
#         )
#         TestStepHandler.wait_for_processes()

#     assert any(
#         [
#             "Starting execution with step handler TestStepHandler" in event.message
#             for event in result.event_list
#         ]
#     )
#     assert result.success
#     assert TestStepHandler.verify_step_count == 3


def dynamic_job_resource_init_failure():
    @op(out=DynamicOut())
    def source():
        for i in range(3):
            yield DynamicOutput(i, mapping_key=str(i))

    @resource(config_schema={"path": str})
    def may_raise(init_context):

        count = -1
        with open(os.path.join(init_context.resource_config["path"], "the_count.pkl"), "rb") as f:
            count = pickle.load(f)
            if count > 0:
                raise Exception("oof")
        with open(os.path.join(init_context.resource_config["path"], "the_count.pkl"), "wb") as f:
            count += 1
            pickle.dump(count, f)
        return None

    @op(required_resource_keys={"foo"})
    def may_fail(x):
        return x

    @op
    def consumer(x):
        return 4

    @job(executor_def=test_step_delegating_executor, resource_defs={"foo": may_raise})
    def the_job():
        consumer(source().map(may_fail).collect())

    return the_job


def dynamic_job_op_failure():
    @op(out=DynamicOut())
    def source():
        for i in range(3):
            yield DynamicOutput(i, mapping_key=str(i))

    @op(config_schema={"path": str})
    def may_fail(context, x):
        count = -1
        with open(os.path.join(context.op_config["path"], "the_count.pkl"), "rb") as f:
            count = pickle.load(f)
            if count > 0:
                raise Exception("oof")
        with open(os.path.join(context.op_config["path"], "the_count.pkl"), "wb") as f:
            count += 1
            pickle.dump(count, f)
        return None

    @op
    def consumer(x):
        return 4

    @job(executor_def=test_step_delegating_executor)
    def the_job():
        consumer(source().map(may_fail).collect())

    return the_job


retry_jobs = [
    (
        dynamic_job_resource_init_failure,
        lambda temp_dir: {"resources": {"foo": {"config": {"path": temp_dir}}}},
    ),
    (
        dynamic_job_op_failure,
        lambda temp_dir: {"ops": {"may_fail": {"config": {"path": temp_dir}}}},
    ),
]


def test_dynamic_failure_retry():
    for job_fn, config_fn in retry_jobs:
        with TemporaryDirectory() as temp_dir:
            with instance_for_test(temp_dir=temp_dir) as instance:
                TestStepHandler.reset()
                with open(os.path.join(temp_dir, "the_count.pkl"), "wb") as f:
                    pickle.dump(0, f)
                result = execute_job(
                    reconstructable(job_fn),
                    instance,
                    run_config=config_fn(temp_dir),
                )
                TestStepHandler.wait_for_processes()
                assert not result.success
                assert len(result.get_step_success_events()) == 2
                step_success_keys = [event.step_key for event in result.get_step_success_events()]
                assert "source" in step_success_keys
                assert any(
                    [re.match(r"may_fail\[\d\]", step_key) for step_key in step_success_keys]
                )
                assert len(result.get_step_failure_events()) == 2
                step_failure_keys = [event.step_key for event in result.get_step_failure_events()]
                assert all(
                    [re.match(r"may_fail\[\d\]", step_key) for step_key in step_failure_keys]
                )
                with open(os.path.join(temp_dir, "the_count.pkl"), "wb") as f:
                    pickle.dump(-2, f)
                TestStepHandler.reset()
                retry_result = execute_job(
                    reconstructable(job_fn),
                    instance,
                    run_config=config_fn(temp_dir),
                    reexecution_options=ReexecutionOptions.from_failure(
                        run_id=result.run_id, instance=instance
                    ),
                )
                TestStepHandler.wait_for_processes()
                assert retry_result.success
                assert len(retry_result.get_step_success_events()) == 3
                step_success_keys = [
                    event.step_key for event in retry_result.get_step_success_events()
                ]
                assert "consumer" in step_success_keys
                step_success_keys.remove("consumer")
                assert all(
                    [re.match(r"may_fail\[\d\]", step_key) for step_key in step_success_keys]
                )
