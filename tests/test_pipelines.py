"""
Testes para plugins e DAGs do pipeline de orquestração de microserviços.

Nota: testes de estrutura de DAG (DagBag) requerem Linux/Mac ou o container
Docker, pois o Airflow 2.8.x usa o módulo `fcntl` que não existe no Windows.
Execute esses testes dentro do container:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/ -v
"""

from __future__ import annotations

import sys
import pytest
from unittest.mock import MagicMock, patch

# ─── Testes de Estrutura das DAGs ─────────────────────────────────────────────
# Skippados automaticamente no Windows — rodar no container Docker.


@pytest.mark.skipif(
    sys.platform == "win32", reason="fcntl nao existe no Windows — rode no container"
)
class TestDagStructure:

    @pytest.fixture(scope="class")
    def dagbag(self):
        import os
        from airflow.models import DagBag

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        dags_folder = os.path.join(root, "dags")
        plugins_dir = os.path.join(root, "plugins")
        for p in [dags_folder, plugins_dir, root]:
            if p not in sys.path:
                sys.path.insert(0, p)
        return DagBag(dag_folder=dags_folder, include_examples=False, safe_mode=False)

    def test_no_import_errors(self, dagbag):
        assert dagbag.import_errors == {}, f"Erros: {dagbag.import_errors}"

    def test_expected_dags_loaded(self, dagbag):
        expected = {"microservices_orchestration_pipeline", "service_recovery_pipeline"}
        missing = expected - set(dagbag.dag_ids)
        assert not missing, f"DAGs ausentes: {missing}"

    def test_orchestration_dag_config(self, dagbag):
        dag = dagbag.dags.get("microservices_orchestration_pipeline")
        assert dag is not None
        assert dag.max_active_runs == 2
        assert dag.catchup is False
        assert "microservices" in dag.tags

    def test_recovery_dag_config(self, dagbag):
        dag = dagbag.dags.get("service_recovery_pipeline")
        assert dag is not None
        assert dag.max_active_runs == 1
        assert "recovery" in dag.tags

    def test_orchestration_required_tasks(self, dagbag):
        dag = dagbag.dags.get("microservices_orchestration_pipeline")
        task_ids = set(dag.task_ids)
        required = {
            "start",
            "route_based_on_health",
            "end",
            "register_execution_metrics",
        }
        missing = required - task_ids
        assert not missing, f"Tasks ausentes: {missing}"

    def test_recovery_required_tasks(self, dagbag):
        dag = dagbag.dags.get("service_recovery_pipeline")
        task_ids = set(dag.task_ids)
        required = {"start", "resolve_recovery_params", "end"}
        missing = required - task_ids
        assert not missing, f"Tasks ausentes: {missing}"

    def test_no_cycles(self, dagbag):
        for dag_id, dag in dagbag.dags.items():
            try:
                dag.topological_sort()
            except Exception as e:
                pytest.fail(f"Ciclo em {dag_id}: {e}")

    def test_default_args_retries(self, dagbag):
        dag = dagbag.dags.get("microservices_orchestration_pipeline")
        assert dag.default_args["retries"] == 2


# ─── Sintaxe das DAGs (funciona no Windows) ───────────────────────────────────


class TestDagSyntax:
    """Verifica a sintaxe das DAGs sem importar o Airflow completo."""

    def _get_dag_files(self):
        import os

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        dags = os.path.join(root, "dags")
        return [
            os.path.join(dags, f)
            for f in os.listdir(dags)
            if f.endswith(".py") and not f.startswith("_")
        ]

    def test_dag_files_exist(self):
        import os

        files = self._get_dag_files()
        names = [os.path.basename(f) for f in files]
        assert "microservices_orchestration_pipeline.py" in names
        assert "service_recovery_pipeline.py" in names

    def test_dag_files_valid_python(self):
        import ast
        import os

        for path in self._get_dag_files():
            with open(path, encoding="utf-8") as f:
                source = f.read()
            try:
                ast.parse(source)
            except SyntaxError as e:
                pytest.fail(f"Erro de sintaxe em {os.path.basename(path)}: {e}")

    def test_dag_ids_in_source(self):
        import os

        expected = {  # noqa
            "microservices_orchestration_pipeline.py": "microservices_orchestration_pipeline",  # noqa
            "service_recovery_pipeline.py": "service_recovery_pipeline",
        }
        for path in self._get_dag_files():
            name = os.path.basename(path)
            if name in expected:
                with open(path, encoding="utf-8") as f:
                    source = f.read()
                assert (
                    expected[name] in source
                ), f"dag_id '{expected[name]}' não encontrado em {name}"


# ─── MicroserviceHealthSensor ─────────────────────────────────────────────────


class TestMicroserviceHealthSensor:

    def _make_sensor(self, **kwargs):
        from sensors.microservice_health_sensor import MicroserviceHealthSensor

        return MicroserviceHealthSensor(
            task_id="test_health",
            service_url="http://orders-service:8001/health",
            **kwargs,
        )

    @patch("requests.get")
    def test_poke_healthy(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: {"status": "healthy"}
        )
        assert self._make_sensor(expected_status="healthy").poke({}) is True

    @patch("requests.get")
    def test_poke_wrong_status(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: {"status": "starting"}
        )
        assert self._make_sensor(expected_status="healthy").poke({}) is False

    @patch("requests.get")
    def test_poke_service_down(self, mock_get):
        mock_get.return_value = MagicMock(status_code=503)
        assert self._make_sensor().poke({}) is False

    @patch("requests.get")
    def test_poke_connection_error(self, mock_get):
        import requests as req

        mock_get.side_effect = req.exceptions.ConnectionError("refused")
        assert self._make_sensor().poke({}) is False

    @patch("requests.get")
    def test_poke_dependencies_all_healthy(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "status": "healthy",
                "dependencies": {
                    "database": {"status": "healthy"},
                    "cache": {"status": "healthy"},
                },
            },
        )
        assert (
            self._make_sensor(required_dependencies=["database", "cache"]).poke({})
            is True
        )

    @patch("requests.get")
    def test_poke_dependency_unhealthy(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "status": "healthy",
                "dependencies": {
                    "database": {"status": "healthy"},
                    "cache": {"status": "degraded"},
                },
            },
        )
        assert (
            self._make_sensor(required_dependencies=["database", "cache"]).poke({})
            is False
        )

    def test_template_fields(self):
        from sensors.microservice_health_sensor import MicroserviceHealthSensor

        assert "service_url" in MicroserviceHealthSensor.template_fields

    def test_soft_fail(self):
        assert self._make_sensor(soft_fail=True).soft_fail is True


# ─── ServiceRegistryHook ──────────────────────────────────────────────────────


class TestServiceRegistryHook:

    @patch("requests.get")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_list_active_services(self, mock_conn, mock_get):
        mock_conn.return_value = MagicMock(host="consul", port=8500)
        services_response = MagicMock(
            status_code=200,
            json=lambda: {
                "orders-service": ["event-producer"],
                "payments-service": ["event-producer"],
            },
        )
        detail_response = MagicMock(
            status_code=200,
            json=lambda: [
                {
                    "ServiceAddress": "10.0.0.1",
                    "ServicePort": 8001,
                    "ServiceMeta": {
                        "events_endpoint": "/api/v1/events/export",
                        "priority": "high",
                    },
                }
            ],
        )
        mock_get.side_effect = [services_response, detail_response, detail_response]

        from hooks.service_registry_hook import ServiceRegistryHook

        services = ServiceRegistryHook(conn_id="service_registry").list_active_services(
            tag="event-producer"
        )
        assert len(services) == 2
        assert all("name" in s and "base_url" in s for s in services)


# ─── SparkMicroserviceOperator ────────────────────────────────────────────────


class TestSparkMicroserviceOperator:

    def _make_op(self, **kwargs):
        from operators.spark_microservice_operator import SparkMicroserviceOperator

        return SparkMicroserviceOperator(
            task_id="test_spark",
            application="spark_jobs/parse_events.py",
            spark_master="spark://localhost:7077",
            **kwargs,
        )

    def test_instantiation(self):
        op = self._make_op(num_executors=4, executor_memory="4g")
        assert op.application == "spark_jobs/parse_events.py"
        assert op.num_executors == 4

    def test_build_command_basics(self):
        cmd = self._make_op(
            num_executors=2, executor_memory="2g"
        )._build_spark_submit_cmd()
        assert "spark-submit" in cmd
        assert "spark://localhost:7077" in cmd
        assert "2" in cmd and "2g" in cmd

    def test_build_command_with_conf(self):
        cmd = self._make_op(
            conf={"spark.executor.cores": "2"}
        )._build_spark_submit_cmd()
        values = [cmd[i + 1] for i, x in enumerate(cmd) if x == "--conf"]
        assert any("spark.executor.cores=2" in c for c in values)

    def test_build_command_with_args(self):
        cmd = self._make_op(
            application_args=["--input", "s3://bucket/data"]
        )._build_spark_submit_cmd()
        assert "--input" in cmd and "s3://bucket/data" in cmd

    def test_extract_app_id(self):
        line = (
            "INFO SparkContext: Submitted application: application_1704067200000_0042"
        )
        assert self._make_op()._extract_app_id(line) == "application_1704067200000_0042"

    def test_extract_app_id_not_found(self):
        assert self._make_op()._extract_app_id("random log") is None

    def test_on_kill_with_job_id(self):
        op = self._make_op()
        op._job_id = "application_1704067200000_0001"
        with patch.object(op, "_attempt_job_cancellation") as mock_cancel:
            op.on_kill()
            mock_cancel.assert_called_once()

    def test_on_kill_without_job_id(self):
        op = self._make_op()
        op._job_id = None
        op.on_kill()


# ─── Spark Jobs ───────────────────────────────────────────────────────────────


class TestSparkJobs:

    def test_parse_events_args(self):
        sys.argv = [
            "parse_events.py",
            "--input",
            "s3://bucket/raw",
            "--output",
            "s3://bucket/parsed",
            "--run-id",
            "test_001",
            "--mode",
            "normal",
            "--services",
            "orders-service",
        ]
        from spark_jobs.parse_events import parse_args

        args = parse_args()
        assert args.input == "s3://bucket/raw"
        assert args.run_id == "test_001"

    def test_parse_events_schemas_complete(self):
        from spark_jobs.parse_events import SCHEMAS

        expected = {
            "orders-service",
            "payments-service",
            "inventory-service",
            "shipping-service",
        }
        assert expected == set(SCHEMAS.keys())
        for service, schema in SCHEMAS.items():
            names = [f.name for f in schema.fields]
            assert "event_id" in names
            assert "timestamp" in names
            assert "event_type" in names

    def test_aggregate_metrics_args(self):
        sys.argv = [
            "aggregate_metrics.py",
            "--input",
            "s3://bucket/enriched",
            "--output",
            "s3://bucket/metrics",
            "--run-id",
            "test_001",
            "--window",
            "1h",
        ]
        from spark_jobs.aggregate_metrics import parse_args

        args = parse_args()
        assert args.window == "1h"
        assert args.mode == "normal"
