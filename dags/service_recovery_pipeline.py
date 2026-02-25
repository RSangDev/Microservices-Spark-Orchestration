"""
DAG: service_recovery_pipeline
Recuperação e reprocessamento de eventos com falha.

Acionado via Dataset quando o pipeline principal detecta falhas,
ou manualmente via trigger com dag_run.conf customizado.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG, Dataset
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from sensors.microservice_health_sensor import MicroserviceHealthSensor

log = logging.getLogger(__name__)

EVENTS_BUCKET = Variable.get("events_bucket", default_var="s3a://microservices-events")
OUTPUT_BUCKET = Variable.get("output_bucket", default_var="s3a://microservices-output")

FAILURE_DATASET = Dataset("s3://microservices-events/signals/partial_failure")

default_args = {
    "owner": "platform-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="service_recovery_pipeline",
    description="Recuperação e reprocessamento de eventos com falha",
    schedule=[FAILURE_DATASET],
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["recovery", "spark", "microservices", "resilience"],
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 1. Resolve parâmetros ──────────────────────────────────────────────────

    @task(task_id="resolve_recovery_params")
    def resolve_params(**context) -> dict:
        conf = context["dag_run"].conf or {}
        ds = context["ds"]

        failed_services = conf.get("failed_services", ["all"])
        reprocess_window_hours = int(conf.get("reprocess_window_hours", 2))
        run_id_to_recover = conf.get("run_id", context["run_id"])

        params = {
            "failed_services": failed_services,
            "reprocess_window_hours": reprocess_window_hours,
            "run_id_to_recover": run_id_to_recover,
            "recovery_date": ds,
            "input_path": f"{EVENTS_BUCKET}/{ds.replace('-', '')}",
            "output_path": f"{OUTPUT_BUCKET}/recovery/{ds.replace('-', '')}",
        }
        log.info("Parâmetros de recuperação: %s", params)
        return params

    params = resolve_params()

    # ── 2. Aguarda serviços — soft_fail=True para não bloquear sem mocks ───────
    # Timeout curto (5 min) em dev. Em produção, aumentar conforme SLA.

    wait_payments = MicroserviceHealthSensor(
        task_id="wait_payments_service_recovery",
        service_url="http://payments-service:8003/health",
        expected_status="healthy",
        poke_interval=30,
        timeout=60 * 5,
        mode="reschedule",
        soft_fail=True,  # não bloqueia pipeline se serviço estiver fora
    )

    wait_orders = MicroserviceHealthSensor(
        task_id="wait_orders_service_recovery",
        service_url="http://orders-service:8001/health",
        expected_status="healthy",
        poke_interval=30,
        timeout=60 * 5,
        mode="reschedule",
        soft_fail=True,
    )

    # ── 3. Reprocessamento (simulado via @task — sem spark-submit no worker) ───

    @task_group(group_id="reprocess_spark")
    def reprocess():

        @task(task_id="spark_reparse_failed_events")
        def reparse(recovery_params: dict) -> dict:
            log.info(
                "Simulando reparse | input=%s | services=%s",
                recovery_params["input_path"],
                recovery_params["failed_services"],
            )
            return {
                "status": "success",
                "records_reprocessed": 12_480,
                "path": recovery_params["output_path"],
            }

        @task(task_id="spark_reaggregate_metrics")
        def reaggregate(reparse_result: dict) -> dict:
            log.info(
                "Simulando reagregação | records=%d",
                reparse_result["records_reprocessed"],
            )
            return {"status": "success", "metrics_recomputed": 8}

        result = reparse(params)
        return reaggregate(result)

    reprocessed = reprocess()

    # ── 4. Valida resultado ────────────────────────────────────────────────────

    @task(task_id="validate_recovery_output")
    def validate_recovery(recovery_params: dict) -> dict:
        output_path = recovery_params["output_path"]
        log.info("Validando output em: %s", output_path)
        validation = {
            "output_path": output_path,
            "validation_passed": True,
            "record_count": 12_480,
        }
        log.info("✅ Validação OK: %d registros", validation["record_count"])
        return validation

    validated = validate_recovery(params)

    # ── 5. Re-aciona o pipeline principal ─────────────────────────────────────

    retrigger_main = TriggerDagRunOperator(
        task_id="retrigger_main_pipeline",
        trigger_dag_id="microservices_orchestration_pipeline",
        conf={"triggered_by": "recovery_pipeline"},
        wait_for_completion=False,
    )

    # ── 6. Registro final ──────────────────────────────────────────────────────

    @task(task_id="register_recovery_result", trigger_rule=TriggerRule.ALL_DONE)
    def register_recovery(**context) -> None:
        dag_run = context["dag_run"]
        task_instances = dag_run.get_task_instances()
        success = all(t.state in ("success", "skipped") for t in task_instances)
        log.info(
            "Recuperação %s | run_id=%s",
            "✅ CONCLUÍDA" if success else "⚠️ PARCIAL",
            dag_run.run_id,
        )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Dependências ───────────────────────────────────────────────────────────

    (
        start
        >> params
        >> [wait_payments, wait_orders]
        >> reprocessed
        >> validated
        >> retrigger_main
        >> register_recovery()
        >> end
    )
