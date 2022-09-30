import pytest

from dagster._core.test_utils import create_test_daemon_workspace_context
from dagster._core.workspace.load_target import EmptyWorkspaceTarget
from dagster._utils.test.postgres_instance import postgres_instance_for_test

from .utils import workspace_load_target


@pytest.fixture
def instance():
    with postgres_instance_for_test(
        __file__,
        "test-postgres-db-docker",
        overrides={
            "run_coordinator": {
                "module": "dagster.core.test_utils",
                "class": "MockedRunCoordinator",
            },
            "run_retries": {"enabled": True},
        },
    ) as instance:
        yield instance


@pytest.fixture
def empty_workspace_context(instance):
    with create_test_daemon_workspace_context(
        workspace_load_target=EmptyWorkspaceTarget(), instance=instance
    ) as workspace_context:
        yield workspace_context


@pytest.fixture
def workspace_context(instance):
    with create_test_daemon_workspace_context(
        workspace_load_target=workspace_load_target(), instance=instance
    ) as workspace:
        yield workspace
