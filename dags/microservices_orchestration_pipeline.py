"""
DAG: microservices_orchestration_pipeline
Orquestração de microserviços com health checks, coleta de eventos e processamento.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.service_event_collector import ServiceEventCollectorOperator
from hooks.service_registry_hook import ServiceRegistryHook

log = logging.getLogger(__name__)

MICROSERVICES = [
    {
        "name": "orders-service",
        "base_url": "http://orders-service:8001",
        "events_endpoint": "/api/v1/events/export",
        "conn_id": "orders_service",
        "priority": "high",
    },
    {
        "name": "inventory-service",
        "base_url": "http://inventory-service:8002",
        "events_endpoint": "/api/v1/events/export",
        "conn_id": "inventory_service",
        "priority": "high",
    },
    {
        "name": "payments-service",
        "base_url": "http://payments-service:8003",
        "events_endpoint": "/api/v1/events/export",
        "conn_id": "payments_service",
        "priority": "critical",
    },
    {
        "name": "shipping-service",
        "base_url": "http://shipping-service:8004",
        "events_endpoint": "/api/v1/events/export",
        "conn_id": "shipping_service",
        "priority": "medium",
    },
]

SPARK_MASTER = Variable.get("spark_master", default_var="spark://spark-master:7077")
EVENTS_BUCKET = Variable.get("events_bucket", default_var="s3a://microservices-events")
OUTPUT_BUCKET = Variable.get("output_bucket", default_var="s3a://microservices-output")

default_args = {
    "owner": "platform-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="microservices_orchestration_pipeline",
    description="Orquestração de microserviços + Spark para processamento de eventos",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=2,
    tags=["microservices", "spark", "events", "platform"],
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 1. Health Checks ───────────────────────────────────────────────────────

    @task_group(group_id="health_checks")
    def check_all_services():

        @task(task_id="get_service_list")
        def get_service_list() -> list[dict]:
            try:
                registry = ServiceRegistryHook(conn_id="service_registry")
                services = registry.list_active_services(tag="event-producer")
                return services if services else MICROSERVICES
            except Exception:
                log.warning(
                    "Service Registry indisponível. Usando configuração estática."
                )
                return MICROSERVICES

        @task
        def check_service_health(service_config: dict) -> dict:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            name = service_config["name"]
            base_url = service_config["base_url"]
            session = requests.Session()
            session.mount(
                "http://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5))
            )

            try:
                resp = session.get(f"{base_url}/health", timeout=10)
                resp.raise_for_status()
                log.info("✅ %s — saudável", name)
                return {"name": name, "status": "healthy"}
            except Exception as e:
                priority = service_config.get("priority", "medium")
                log.error("❌ %s — falha: %s", name, str(e))
                if priority == "critical":
                    raise RuntimeError(f"Serviço CRÍTICO indisponível: {name}") from e
                return {"name": name, "status": "degraded", "error": str(e)}

        @task(task_id="evaluate_cluster_health")
        def evaluate_cluster_health(health_results: list[dict]) -> str:
            healthy = [r for r in health_results if r.get("status") == "healthy"]
            health_rate = len(healthy) / len(health_results) if health_results else 0
            log.info("Cluster health: %.0f%%", health_rate * 100)
            if health_rate >= 0.5:
                return "full_pipeline"
            raise RuntimeError(
                f"Cluster muito degradado ({health_rate:.0%}). Abortando."
            )

        services = get_service_list()
        results = check_service_health.expand(service_config=services)
        # Retorna task única — sem tupla
        return evaluate_cluster_health(results)

    health_route = check_all_services()

    # ── 2. Branch ──────────────────────────────────────────────────────────────

    def route_pipeline(**context) -> str:
        route = context["ti"].xcom_pull(
            task_ids="health_checks.evaluate_cluster_health"
        )
        return (
            "collect_events.full_collect"
            if route == "full_pipeline"
            else "collect_events.partial_collect"
        )

    branch = BranchPythonOperator(
        task_id="route_based_on_health",
        python_callable=route_pipeline,
    )

    # ── 3. Coleta de Eventos ───────────────────────────────────────────────────

    @task_group(group_id="collect_events")
    def collect_events():

        full = ServiceEventCollectorOperator(
            task_id="full_collect",
            services=MICROSERVICES,
            output_path="{{ ds_nodash }}/{{ ts_nodash }}",
            mode="full",
            batch_size=10_000,
            compression="snappy",
        )

        partial = ServiceEventCollectorOperator(
            task_id="partial_collect",
            services=[
                s for s in MICROSERVICES if s["priority"] in ("critical", "high")
            ],
            output_path="{{ ds_nodash }}/{{ ts_nodash }}",
            mode="partial",
            batch_size=5_000,
            compression="snappy",
        )

        # Ponto de junção: garante que o task_group retorna um único nó
        join = EmptyOperator(
            task_id="collect_done",
            trigger_rule=TriggerRule.ONE_SUCCESS,
        )

        [full, partial] >> join
        return join

    collected = collect_events()

    # ── 4. Processamento Spark (simulado via @task) ────────────────────────────

    @task_group(group_id="spark_processing")
    def spark_jobs():

        @task(task_id="spark_parse_and_validate")
        def parse_events(ds: str) -> dict:
            log.info("Simulando parse de eventos | ds=%s", ds)
            return {"job": "parse_events", "status": "success", "records": 48_320}

        @task(task_id="spark_cross_service_join")
        def cross_join(parse_result: dict) -> dict:
            log.info(
                "Simulando cross-service join | records=%d", parse_result["records"]
            )
            return {
                "job": "cross_join",
                "status": "success",
                "records": parse_result["records"],
            }

        @task(task_id="spark_business_aggregations")
        def aggregate(join_result: dict) -> dict:
            log.info("Simulando agregações | records=%d", join_result["records"])
            return {"job": "aggregate", "status": "success", "metrics_computed": 12}

        @task(task_id="spark_anomaly_detection")
        def detect_anomalies(join_result: dict) -> dict:
            log.info("Simulando detecção de anomalias")
            return {
                "job": "anomaly_detection",
                "status": "success",
                "anomalies_found": 3,
            }

        # Ponto de junção para as duas branches paralelas (agg + anomaly)
        spark_done = EmptyOperator(task_id="spark_done")

        parsed = parse_events(ds="{{ ds }}")
        joined = cross_join(parsed)
        agg = aggregate(joined)
        anomaly = detect_anomalies(joined)

        [agg, anomaly] >> spark_done
        return spark_done

    spark_results = spark_jobs()

    # ── 5. Publicação ──────────────────────────────────────────────────────────

    @task_group(group_id="publish_results")
    def publish_to_services():

        @task(task_id="prepare_payloads")
        def prepare_payloads(ds: str) -> list[dict]:
            return [
                {
                    "service": s["name"],
                    "endpoint": f"{s['base_url']}/api/v1/metrics/ingest",
                }
                for s in MICROSERVICES
            ]

        @task(task_id="publish_metrics")
        def publish_metrics(payload: dict) -> dict:
            import requests

            try:
                resp = requests.post(
                    payload["endpoint"], json={"source": "airflow"}, timeout=10
                )
                log.info(
                    "✅ Publicado para %s — HTTP %d",
                    payload["service"],
                    resp.status_code,
                )
                return {"service": payload["service"], "published": True}
            except Exception as e:
                log.warning(
                    "⚠️ Falha ao publicar para %s: %s", payload["service"], str(e)
                )
                return {"service": payload["service"], "published": False}

        payloads = prepare_payloads(ds="{{ ds }}")
        # Retorna a task de expand — é um único objeto XComArg
        return publish_metrics.expand(payload=payloads)

    published = publish_to_services()

    # ── 6. Métricas finais ─────────────────────────────────────────────────────

    @task(task_id="register_execution_metrics", trigger_rule=TriggerRule.ALL_DONE)
    def register_metrics(ds: str, run_id: str, **context) -> dict:
        dag_run = context["dag_run"]
        task_instances = dag_run.get_task_instances()
        failed = [t.task_id for t in task_instances if t.state == "failed"]
        metrics = {
            "run_id": run_id,
            "execution_date": ds,
            "failed_tasks": len(failed),
            "pipeline_status": "PARTIAL_FAILURE" if failed else "SUCCESS",
        }
        log.info("📊 Métricas: %s", json.dumps(metrics, indent=2))
        return metrics

    execution_metrics = register_metrics(ds="{{ ds }}", run_id="{{ run_id }}")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Dependências ───────────────────────────────────────────────────────────

    (
        start
        >> health_route
        >> branch
        >> collected
        >> spark_results
        >> published
        >> execution_metrics
        >> end
    )
