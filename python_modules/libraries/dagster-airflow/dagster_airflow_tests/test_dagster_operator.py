import unittest
from datetime import datetime
from unittest.mock import patch

import pytest
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from dagster_airflow import DagsterCloudOperator


@pytest.mark.usefixtures("snapshot")
class TestMapboxEmrJobFlowOperator(unittest.TestCase):
    @patch("dagster_airflow.hooks.dagster_hook.DagsterHook.launch_run")
    @patch("dagster_airflow.hooks.dagster_hook.DagsterHook.wait_for_run")
    def test_failed_execute(self, launch_run, wait_for_run):
        dag = DAG(dag_id="anydag", start_date=datetime.now())
        run_config = {}
        task = DagsterCloudOperator(dag=dag, task_id="anytask", run_config=run_config)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        with pytest.raises(AirflowException):
            task.execute(ti.get_template_context())
        launch_run.assert_called_once()
        wait_for_run.assert_called_once()

    # def test_failed_uptime_hours_str_value(self):
    #     dag = DAG(dag_id="anydag", start_date=datetime.now())
    #     job_flow_overrides = {}
    #     with pytest.raises(ValueError) as execinfo:
    #         MapboxEmrJobFlowOperator(
    #             dag=dag,
    #             task_id="anytask",
    #             job_flow_overrides=job_flow_overrides,
    #             uptime_hours="5",
    #         )
    #     assert (
    #         execinfo.value.args[0]
    #         == "'uptime_hours' must be an integer, but 'str' received"
    #     )

    # def test_failed_uptime_hours_neg_value(self):
    #     dag = DAG(dag_id="anydag", start_date=datetime.now())
    #     job_flow_overrides = {}
    #     with pytest.raises(ValueError) as execinfo:
    #         MapboxEmrJobFlowOperator(
    #             dag=dag,
    #             task_id="anytask",
    #             job_flow_overrides=job_flow_overrides,
    #             uptime_hours=-5,
    #         )
    #     assert execinfo.value.args[0] == "'uptime_hours' must be > 0, but -5 received"

    # def test_failed_uptime_hours_flt_value(self):
    #     dag = DAG(dag_id="anydag", start_date=datetime.now())
    #     job_flow_overrides = {}
    #     with pytest.raises(ValueError) as execinfo:
    #         MapboxEmrJobFlowOperator(
    #             dag=dag,
    #             task_id="anytask",
    #             job_flow_overrides=job_flow_overrides,
    #             uptime_hours=5.2,
    #         )
    #     assert (
    #         execinfo.value.args[0]
    #         == "'uptime_hours' must be an integer, but 'float' received"
    #     )

    # @patch("emr_operator.airflow.operator.MapboxEmrHook.wait_for_termination")
    # @patch(
    #     "emr_operator.airflow.operator.MapboxEmrHook.create_job_flow",
    #     return_value={
    #         "AccountId": "234858372212",
    #         "ClusterName": "foo bar",
    #         "Status": "STARTING",
    #         "StartedAt": 1625245818307,
    #         "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:234858372212:cluster/123",
    #         "ClusterId": "123",
    #     },
    # )
    # def test_successful_execute(self, create_job_flow, wait_for_termination):
    #     dag = DAG(dag_id="anydag", start_date=datetime.now())
    #     job_flow_overrides = {"Name": "foo bar"}
    #     task = MapboxEmrJobFlowOperator(
    #         dag=dag, task_id="anytask", job_flow_overrides=job_flow_overrides
    #     )
    #     ti = TaskInstance(task=task, execution_date=datetime.now())
    #     ctx = ti.get_template_context()
    #     result = task.execute(ctx)
    #     create_job_flow.assert_called_once_with(job_flow_overrides, ctx, "anytask", 48)
    #     wait_for_termination.assert_called_once_with(
    #         cluster_id="123", cluster_name=job_flow_overrides["Name"]
    #     )
    #     self.snapshot.assert_match(result)

    # @patch("emr_operator.airflow.operator.MapboxEmrHook.wait_for_termination")
    # @patch(
    #     "emr_operator.airflow.operator.MapboxEmrHook.create_job_flow",
    #     return_value={
    #         "AccountId": "234858372212",
    #         "ClusterName": "foo bar",
    #         "Status": "STARTING",
    #         "StartedAt": 1625245818307,
    #         "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:234858372212:cluster/123",
    #         "ClusterId": "123",
    #     },
    # )
    # def test_successful_execute_with_uptime_hours(
    #     self, create_job_flow, wait_for_termination
    # ):
    #     dag = DAG(dag_id="anydag", start_date=datetime.now())
    #     job_flow_overrides = {"Name": "foo bar"}
    #     task = MapboxEmrJobFlowOperator(
    #         dag=dag,
    #         task_id="anytask",
    #         job_flow_overrides=job_flow_overrides,
    #         uptime_hours=5,
    #     )
    #     ti = TaskInstance(task=task, execution_date=datetime.now())
    #     ctx = ti.get_template_context()
    #     result = task.execute(ctx)
    #     create_job_flow.assert_called_once_with(job_flow_overrides, ctx, "anytask", 5)
    #     wait_for_termination.assert_called_once_with(
    #         cluster_id="123", cluster_name=job_flow_overrides["Name"]
    #     )
    #     self.snapshot.assert_match(result)

    # @patch("emr_operator.airflow.operator.MapboxEmrHook.wait_for_termination")
    # @patch("emr_operator.airflow.operator.MapboxEmrHook.wait_for_ready")
    # @patch(
    #     "emr_operator.airflow.operator.MapboxEmrHook.get_config",
    #     return_value={"Instances": {"KeepJobFlowAliveWhenNoSteps": True}},
    # )
    # @patch(
    #     "emr_operator.airflow.operator.MapboxEmrHook.create_job_flow",
    #     return_value={
    #         "AccountId": "234858372212",
    #         "ClusterName": "foo bar",
    #         "Status": "STARTING",
    #         "StartedAt": 1625245818307,
    #         "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:234858372212:cluster/123",
    #         "ClusterId": "123",
    #     },
    # )
    # def test_keep_alive(
    #     self, create_job_flow, get_conf, wait_for_ready, wait_for_termination
    # ):
    #     dag = DAG(dag_id="anydag", start_date=datetime.now())
    #     job_flow_overrides = {"Name": "foo bar"}
    #     task = MapboxEmrJobFlowOperator(
    #         dag=dag, task_id="anytask", job_flow_overrides=job_flow_overrides
    #     )
    #     ti = TaskInstance(task=task, execution_date=datetime.now())
    #     ctx = ti.get_template_context()
    #     result = task.execute(ctx)
    #     create_job_flow.assert_called_once_with(job_flow_overrides, ctx, "anytask", 48)
    #     wait_for_ready.assert_called_once_with(
    #         cluster_name=job_flow_overrides["Name"], cluster_id="123"
    #     )
    #     wait_for_termination.assert_not_called()
    #     self.snapshot.assert_match(result)

    # @patch("emr_operator.airflow.operator.MapboxEmrHook.wait_for_termination")
    # @patch(
    #     "emr_operator.airflow.operator.MapboxEmrHook.create_job_flow",
    #     return_value={
    #         "AccountId": "234858372212",
    #         "ClusterName": "foo bar",
    #         "Status": "STARTING",
    #         "StartedAt": 1625245818307,
    #         "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:234858372212:cluster/123",
    #         "ClusterId": 123,
    #     },
    # )
    # def test_pre_execute(self, create_job_flow, wait_for_termination):
    #     now = datetime.now()
    #     dag = DAG(dag_id="anydag", start_date=now)
    #     task = MapboxEmrJobFlowOperator(
    #         dag=dag,
    #         task_id="anytask",
    #         params={"current": "{{ ds }}"},
    #         job_flow_overrides={"from_params": "{{ params.current }}"},
    #     )
    #     ti = TaskInstance(task=task, execution_date=datetime.now())
    #     ctx = ti.get_template_context()
    #     ti.render_templates(ctx)
    #     # default render substitutes values in from params, but they aren't rendered
    #     self.snapshot.assert_match(
    #         task.job_flow_overrides
    #     )  # == {"from_params": "{{ ds }}"}
    #     task.pre_execute(ctx)
    #     # our pre-execute forces re-rendering of job_flow_overrides
    #     self.assertDictEqual(
    #         task.job_flow_overrides,
    #         {"from_params": now.date().isoformat()},
    #         "job_flow_overrides updated with rendered date",
    #     )
    #     task.execute(ctx)
    #     create_job_flow.assert_called_once_with(
    #         {"from_params": now.date().isoformat()}, ctx, "anytask", 48
    #     )
