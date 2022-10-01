import os
import sys
import time

import pytest

from dagster import (
    Failure,
    Field,
    MetadataEntry,
    Nothing,
    Output,
    String,
    reconstructable,
    DynamicOut,
    DynamicOutput,
    resource,
    graph,
    op,
    job,
    execute_job,
    ReexecutionOptions,
)
from tempfile import TemporaryDirectory
import pickle
from dagster._core.errors import DagsterUnmetExecutorRequirementsError
from dagster._core.instance import DagsterInstance
from dagster._core.storage.compute_log_manager import ComputeIOType
from dagster._core.test_utils import default_mode_def_for_test, instance_for_test
from dagster._legacy import (
    InputDefinition,
    OutputDefinition,
    PresetDefinition,
    execute_pipeline,
    lambda_solid,
    pipeline,
    solid,
)
from dagster._utils import safe_tempfile_path, segfault


def test_diamond_simple_execution():
    result = execute_pipeline(define_diamond_pipeline())
    assert result.success
    assert result.result_for_solid("adder").output_value() == 11


def compute_event(result, solid_name):
    return result.result_for_solid(solid_name).compute_step_events[0]


def test_diamond_multi_execution():
    with instance_for_test() as instance:
        pipe = reconstructable(define_diamond_pipeline)
        result = execute_pipeline(
            pipe,
            run_config={
                "execution": {"multiprocess": {}},
            },
            instance=instance,
        )
        assert result.success

        assert result.result_for_solid("adder").output_value() == 11


def test_explicit_spawn():
    with instance_for_test() as instance:
        pipe = reconstructable(define_diamond_pipeline)
        result = execute_pipeline(
            pipe,
            run_config={
                "execution": {"multiprocess": {"config": {"start_method": {"spawn": {}}}}},
            },
            instance=instance,
        )
        assert result.success

        assert result.result_for_solid("adder").output_value() == 11


@pytest.mark.skipif(os.name == "nt", reason="No forkserver on windows")
def test_forkserver_execution():
    with instance_for_test() as instance:
        pipe = reconstructable(define_diamond_pipeline)
        result = execute_pipeline(
            pipe,
            run_config={
                "execution": {"multiprocess": {"config": {"start_method": {"forkserver": {}}}}},
            },
            instance=instance,
        )
        assert result.success

        assert result.result_for_solid("adder").output_value() == 11


@pytest.mark.skipif(os.name == "nt", reason="No forkserver on windows")
def test_forkserver_preload():
    with instance_for_test() as instance:
        pipe = reconstructable(define_diamond_pipeline)
        result = execute_pipeline(
            pipe,
            run_config={
                "execution": {
                    "multiprocess": {
                        "config": {"start_method": {"forkserver": {"preload_modules": []}}}
                    }
                },
            },
            instance=instance,
        )
        assert result.success

        assert result.result_for_solid("adder").output_value() == 11


def define_diamond_pipeline():
    @lambda_solid
    def return_two():
        return 2

    @lambda_solid(input_defs=[InputDefinition("num")])
    def add_three(num):
        return num + 3

    @lambda_solid(input_defs=[InputDefinition("num")])
    def mult_three(num):
        return num * 3

    @lambda_solid(input_defs=[InputDefinition("left"), InputDefinition("right")])
    def adder(left, right):
        return left + right

    @pipeline(
        preset_defs=[
            PresetDefinition(
                "just_adder",
                {
                    "execution": {"multiprocess": {}},
                    "solids": {"adder": {"inputs": {"left": {"value": 1}, "right": {"value": 1}}}},
                },
                solid_selection=["adder"],
            )
        ],
        mode_defs=[default_mode_def_for_test],
    )
    def diamond_pipeline():
        two = return_two()
        adder(left=add_three(two), right=mult_three(two))

    return diamond_pipeline


def define_in_mem_pipeline():
    @lambda_solid
    def return_two():
        return 2

    @lambda_solid(input_defs=[InputDefinition("num")])
    def add_three(num):
        return num + 3

    @pipeline
    def in_mem_pipeline():
        add_three(return_two())

    return in_mem_pipeline


def define_error_pipeline():
    @lambda_solid
    def should_never_execute(_x):
        assert False  # this should never execute

    @lambda_solid
    def throw_error():
        raise Exception("bad programmer")

    @pipeline(mode_defs=[default_mode_def_for_test])
    def error_pipeline():
        should_never_execute(throw_error())

    return error_pipeline


def test_error_pipeline():
    pipe = define_error_pipeline()
    result = execute_pipeline(pipe, raise_on_error=False)
    assert not result.success


def test_error_pipeline_multiprocess():
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(define_error_pipeline),
            run_config={
                "execution": {"multiprocess": {}},
            },
            instance=instance,
        )
        assert not result.success


def test_mem_storage_error_pipeline_multiprocess():
    with instance_for_test() as instance:
        with pytest.raises(
            DagsterUnmetExecutorRequirementsError,
            match="your pipeline includes solid outputs that will not be stored somewhere where other processes can retrieve them.",
        ):
            execute_pipeline(
                reconstructable(define_in_mem_pipeline),
                run_config={"execution": {"multiprocess": {}}},
                instance=instance,
                raise_on_error=False,
            )


def test_invalid_instance():
    result = execute_pipeline(
        reconstructable(define_diamond_pipeline),
        run_config={"execution": {"multiprocess": {}}},
        instance=DagsterInstance.ephemeral(),
        raise_on_error=False,
    )
    assert not result.success
    assert len(result.event_list) == 1
    assert result.event_list[0].is_failure
    assert (
        result.event_list[0].pipeline_failure_data.error.cls_name
        == "DagsterUnmetExecutorRequirementsError"
    )
    assert "non-ephemeral instance" in result.event_list[0].pipeline_failure_data.error.message


def test_no_handle():
    result = execute_pipeline(
        define_diamond_pipeline(),
        run_config={"execution": {"multiprocess": {}}},
        instance=DagsterInstance.ephemeral(),
        raise_on_error=False,
    )
    assert not result.success
    assert len(result.event_list) == 1
    assert result.event_list[0].is_failure
    assert (
        result.event_list[0].pipeline_failure_data.error.cls_name
        == "DagsterUnmetExecutorRequirementsError"
    )
    assert "is not reconstructable" in result.event_list[0].pipeline_failure_data.error.message


def test_solid_selection():
    with instance_for_test() as instance:
        pipe = reconstructable(define_diamond_pipeline)

        result = execute_pipeline(pipe, preset="just_adder", instance=instance)

        assert result.success

        assert result.result_for_solid("adder").output_value() == 2


def define_subdag_pipeline():
    @solid(config_schema=Field(String))
    def waiter(context):
        done = False
        while not done:
            time.sleep(0.15)
            if os.path.isfile(context.solid_config):
                return

    @solid(
        input_defs=[InputDefinition("after", Nothing)],
        config_schema=Field(String),
    )
    def writer(context):
        with open(context.solid_config, "w", encoding="utf8") as fd:
            fd.write("1")
        return

    @lambda_solid(
        input_defs=[InputDefinition("after", Nothing)],
        output_def=OutputDefinition(Nothing),
    )
    def noop():
        pass

    @pipeline(mode_defs=[default_mode_def_for_test])
    def separate():
        waiter()
        a = noop.alias("noop_1")()
        b = noop.alias("noop_2")(a)
        c = noop.alias("noop_3")(b)
        writer(c)

    return separate


def test_separate_sub_dags():
    with instance_for_test() as instance:
        pipe = reconstructable(define_subdag_pipeline)

        with safe_tempfile_path() as filename:
            result = execute_pipeline(
                pipe,
                run_config={
                    "execution": {"multiprocess": {"config": {"max_concurrent": 2}}},
                    "solids": {
                        "waiter": {"config": filename},
                        "writer": {"config": filename},
                    },
                },
                instance=instance,
            )

        assert result.success

        # this test is to ensure that the chain of noop -> noop -> noop -> writer is not blocked by waiter
        order = [
            str(event.solid_handle) for event in result.step_event_list if event.is_step_success
        ]

        # the writer and waiter my finish in different orders so just ensure the proceeding chain
        assert order[0:3] == ["noop_1", "noop_2", "noop_3"]


def test_ephemeral_event_log():
    with instance_for_test(
        overrides={
            "event_log_storage": {
                "module": "dagster._core.storage.event_log",
                "class": "InMemoryEventLogStorage",
            }
        }
    ) as instance:
        pipe = reconstructable(define_diamond_pipeline)
        # override event log to in memory

        result = execute_pipeline(
            pipe,
            run_config={
                "execution": {"multiprocess": {}},
            },
            instance=instance,
        )
        assert result.success

        assert result.result_for_solid("adder").output_value() == 11


@solid(
    output_defs=[
        OutputDefinition(name="option_1", is_required=False),
        OutputDefinition(name="option_2", is_required=False),
    ]
)
def either_or(_context):
    yield Output(1, "option_1")


@lambda_solid
def echo(x):
    return x


@pipeline(mode_defs=[default_mode_def_for_test])
def optional_stuff():
    option_1, option_2 = either_or()
    echo(echo(option_1))
    echo(echo(option_2))


def test_optional_outputs():
    with instance_for_test() as instance:
        single_result = execute_pipeline(optional_stuff)
        assert single_result.success
        assert not [event for event in single_result.step_event_list if event.is_step_failure]
        assert len([event for event in single_result.step_event_list if event.is_step_skipped]) == 2

        multi_result = execute_pipeline(
            reconstructable(optional_stuff),
            run_config={
                "execution": {"multiprocess": {}},
            },
            instance=instance,
        )
        assert multi_result.success
        assert not [event for event in multi_result.step_event_list if event.is_step_failure]
        assert len([event for event in multi_result.step_event_list if event.is_step_skipped]) == 2


@lambda_solid
def throw():
    raise Failure(
        description="it Failure",
        metadata_entries=[MetadataEntry("label", value="text")],
    )


@pipeline(mode_defs=[default_mode_def_for_test])
def failure():
    throw()


def test_failure_multiprocessing():
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(failure),
            run_config={
                "execution": {"multiprocess": {}},
            },
            instance=instance,
            raise_on_error=False,
        )
        assert not result.success
        failure_data = result.result_for_solid("throw").failure_data
        assert failure_data
        assert failure_data.error.cls_name == "Failure"

        # hard coded
        assert failure_data.user_failure_data.label == "intentional-failure"
        # from Failure
        assert failure_data.user_failure_data.description == "it Failure"
        assert failure_data.user_failure_data.metadata_entries[0].label == "label"
        assert failure_data.user_failure_data.metadata_entries[0].entry_data.text == "text"


@solid
def sys_exit(context):
    context.log.info("Informational message")
    print("Crashy output to stdout")  # pylint: disable=print-call
    sys.stdout.flush()
    os._exit(1)  # pylint: disable=W0212


@pipeline(mode_defs=[default_mode_def_for_test])
def sys_exit_pipeline():
    sys_exit()


@pytest.mark.skipif(os.name == "nt", reason="Different crash output on Windows: See issue #2791")
def test_crash_multiprocessing():
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(sys_exit_pipeline),
            run_config={
                "execution": {"multiprocess": {}},
            },
            instance=instance,
            raise_on_error=False,
        )
        assert not result.success
        failure_data = result.result_for_solid("sys_exit").failure_data
        assert failure_data
        assert failure_data.error.cls_name == "ChildProcessCrashException"

        assert failure_data.user_failure_data is None

        assert (
            "Crashy output to stdout"
            in instance.compute_log_manager.read_logs_file(
                result.run_id, "sys_exit", ComputeIOType.STDOUT
            ).data
        )

        # The argument to sys.exit won't (reliably) make it to the compute logs for stderr b/c the
        # LocalComputeLogManger is in-process -- documenting this behavior here though we may want to
        # change it

        # assert (
        #     'Crashy output to stderr'
        #     not in instance.compute_log_manager.read_logs_file(
        #         result.run_id, 'sys_exit', ComputeIOType.STDERR
        #     ).data
        # )


# segfault test
@solid
def segfault_solid(context):
    context.log.info("Informational message")
    print("Crashy output to stdout")  # pylint: disable=print-call
    segfault()


@pipeline(mode_defs=[default_mode_def_for_test])
def segfault_pipeline():
    segfault_solid()


@pytest.mark.skipif(os.name == "nt", reason="Different exception on Windows: See issue #2791")
def test_crash_hard_multiprocessing():
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(segfault_pipeline),
            run_config={
                "execution": {"multiprocess": {}},
            },
            instance=instance,
            raise_on_error=False,
        )
        assert not result.success
        failure_data = result.result_for_solid("segfault_solid").failure_data
        assert failure_data
        assert failure_data.error.cls_name == "ChildProcessCrashException"

        assert failure_data.user_failure_data is None

        # Neither the stderr not the stdout spew will (reliably) make it to the compute logs --
        # documenting this behavior here though we may want to change it

        # assert (
        #     'Crashy output to stdout'
        #     not in instance.compute_log_manager.read_logs_file(
        #         result.run_id, 'segfault_solid', ComputeIOType.STDOUT
        #     ).data
        # )

        # assert (
        #     instance.compute_log_manager.read_logs_file(
        #         result.run_id, 'sys_exit', ComputeIOType.STDERR
        #     ).data
        #     is None
        # )


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

    @graph
    def the_graph():
        consumer(source().map(may_fail).collect())

    return the_graph.to_job(resource_defs={"foo": may_raise})


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

    @graph
    def the_graph():
        consumer(source().map(may_fail).collect())

    return the_graph.to_job()


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


def keys_match(keys, the_list):
    return set(keys) == set(the_list)


def test_dynamic_failure_retry_multiprocess():
    for job_fn, config_fn in retry_jobs:
        with TemporaryDirectory() as temp_dir:
            with instance_for_test(temp_dir=temp_dir) as instance:
                with open(os.path.join(temp_dir, "the_count.pkl"), "wb") as f:
                    pickle.dump(0, f)
                result = execute_job(
                    reconstructable(job_fn),
                    instance,
                    run_config=config_fn(temp_dir),
                )
                assert not result.success
                assert len(result.get_step_success_events()) == 2
                assert keys_match(
                    ["source", "may_fail[0]"],
                    [event.step_key for event in result.get_step_success_events()],
                )
                assert len(result.get_step_failure_events()) == 2
                assert keys_match(
                    ["may_fail[1]", "may_fail[2]"],
                    [event.step_key for event in result.get_step_failure_events()],
                )
                with open(os.path.join(temp_dir, "the_count.pkl"), "wb") as f:
                    pickle.dump(-2, f)
                retry_result = execute_job(
                    reconstructable(job_fn),
                    instance,
                    run_config=config_fn(temp_dir),
                    reexecution_options=ReexecutionOptions.from_failure(
                        run_id=result.run_id, instance=instance
                    ),
                )
                assert len(retry_result.get_step_success_events()) == 3
                assert keys_match(
                    ["may_fail[1]", "may_fail[2]", "consumer"],
                    [event.step_key for event in retry_result.get_step_success_events()],
                )
