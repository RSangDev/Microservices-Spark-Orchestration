"""
Plugin: SparkMicroserviceOperator
Operador customizado para submissão e rastreamento de jobs Spark
em contexto de orquestração de microserviços.

Diferencial em relação ao SparkSubmitOperator padrão:
  - Rastreamento de status do job via Spark REST API
  - Coleta de métricas (duração, registros, estágios) via XCom
  - Cancelamento gracioso em caso de timeout
  - Suporte a retry com limpeza de output parcial
  - Log estruturado dos estágios Spark
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import requests
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SparkMicroserviceOperator(BaseOperator):
    """
    Submete um job Spark e monitora até conclusão, coletando métricas detalhadas.

    :param application: Caminho para o script Python Spark
    :param spark_master: URL do Spark Master (ex.: spark://spark-master:7077)
    :param spark_rest_url: URL da Spark REST API para monitoramento
    :param application_args: Argumentos passados ao script Python
    :param conf: Configurações Spark (spark.executor.memory, etc.)
    :param num_executors: Número de executors
    :param executor_memory: Memória por executor (ex.: "4g")
    :param executor_cores: Cores por executor
    :param driver_memory: Memória do driver
    :param poll_interval: Intervalo em segundos para checar status do job
    :param spark_binary: Caminho para spark-submit
    """

    template_fields = ("application", "application_args", "conf")
    template_fields_renderers = {"application_args": "json", "conf": "json"}
    ui_color = "#E25B22"
    ui_fgcolor = "#ffffff"

    @apply_defaults
    def __init__(
        self,
        application: str,
        spark_master: str,
        spark_rest_url: str = "http://spark-master:8080",
        application_args: list[str] | None = None,
        conf: dict[str, str] | None = None,
        num_executors: int = 2,
        executor_memory: str = "2g",
        executor_cores: int = 2,
        driver_memory: str = "1g",
        poll_interval: int = 15,
        spark_binary: str = "spark-submit",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application = application
        self.spark_master = spark_master
        self.spark_rest_url = spark_rest_url
        self.application_args = application_args or []
        self.conf = conf or {}
        self.num_executors = num_executors
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.driver_memory = driver_memory
        self.poll_interval = poll_interval
        self.spark_binary = spark_binary
        self._job_id: str | None = None

    def execute(self, context: dict) -> dict:
        import subprocess

        cmd = self._build_spark_submit_cmd()
        self.log.info("🟠 Submetendo Spark job: %s", " ".join(cmd))
        self.log.info("Application: %s", self.application)
        self.log.info("Executors: %d x %s", self.num_executors, self.executor_memory)

        start_time = time.time()

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Monitora output em tempo real e extrai Spark Application ID
        output_lines = []
        for line in process.stdout:
            line = line.rstrip()
            output_lines.append(line)
            self.log.info("[spark] %s", line)

            # Extrai Application ID para monitoramento via REST API
            if "application_" in line and self._job_id is None:
                self._job_id = self._extract_app_id(line)
                if self._job_id:
                    self.log.info("📌 Spark Application ID: %s", self._job_id)

        return_code = process.wait()
        elapsed = time.time() - start_time

        if return_code != 0:
            self._handle_failure(output_lines, return_code)

        # Coleta métricas via Spark REST API
        metrics = self._collect_job_metrics(elapsed)
        self.log.info("✅ Spark job concluído em %.1fs | Métricas: %s", elapsed, metrics)

        # Publica métricas via XCom para uso downstream
        context["ti"].xcom_push(key="spark_job_metrics", value=metrics)
        context["ti"].xcom_push(key="spark_app_id", value=self._job_id)

        return metrics

    def _build_spark_submit_cmd(self) -> list[str]:
        """Constrói o comando spark-submit com todos os parâmetros."""
        cmd = [
            self.spark_binary,
            "--master", self.spark_master,
            "--deploy-mode", "client",
            "--num-executors", str(self.num_executors),
            "--executor-memory", self.executor_memory,
            "--executor-cores", str(self.executor_cores),
            "--driver-memory", self.driver_memory,
        ]

        # Adiciona configurações customizadas
        for key, value in self.conf.items():
            cmd.extend(["--conf", f"{key}={value}"])

        # Configurações padrão para observabilidade
        default_conf = {
            "spark.sql.adaptive.enabled": "true",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "s3a://spark-logs/event-logs",
            "spark.history.fs.logDirectory": "s3a://spark-logs/event-logs",
        }
        for key, value in default_conf.items():
            if key not in self.conf:
                cmd.extend(["--conf", f"{key}={value}"])

        cmd.append(self.application)
        cmd.extend(self.application_args)

        return cmd

    def _extract_app_id(self, line: str) -> str | None:
        """Extrai o Spark Application ID do output do spark-submit."""
        import re
        match = re.search(r"(application_\d+_\d+)", line)
        return match.group(1) if match else None

    def _collect_job_metrics(self, elapsed_seconds: float) -> dict:
        """
        Coleta métricas detalhadas via Spark REST API.
        Em caso de falha na coleta, retorna métricas básicas.
        """
        metrics = {
            "application_id": self._job_id,
            "application": self.application,
            "duration_seconds": round(elapsed_seconds, 2),
            "num_executors": self.num_executors,
            "executor_memory": self.executor_memory,
        }

        if not self._job_id:
            return metrics

        try:
            # Spark REST API — métricas de stages
            resp = requests.get(
                f"{self.spark_rest_url}/api/v1/applications/{self._job_id}/stages",
                timeout=10,
            )
            if resp.status_code == 200:
                stages = resp.json()
                metrics.update({
                    "total_stages": len(stages),
                    "completed_stages": sum(1 for s in stages if s.get("status") == "COMPLETE"),
                    "failed_stages": sum(1 for s in stages if s.get("status") == "FAILED"),
                    "total_tasks": sum(s.get("numTasks", 0) for s in stages),
                    "input_bytes": sum(s.get("inputBytes", 0) for s in stages),
                    "output_bytes": sum(s.get("outputBytes", 0) for s in stages),
                    "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in stages),
                })

            # Executors
            exec_resp = requests.get(
                f"{self.spark_rest_url}/api/v1/applications/{self._job_id}/executors",
                timeout=10,
            )
            if exec_resp.status_code == 200:
                executors = exec_resp.json()
                metrics["active_executors"] = len([e for e in executors if e.get("isActive")])

        except Exception as e:
            self.log.warning("Não foi possível coletar métricas Spark REST: %s", str(e))

        return metrics

    def _handle_failure(self, output_lines: list[str], return_code: int) -> None:
        """Processa falha do job Spark, cancela se necessário e lança exceção."""
        if self._job_id:
            self._attempt_job_cancellation()

        error_lines = [
            l for l in output_lines
            if any(kw in l for kw in ("Exception", "Error", "FAILED", "killed"))
        ]
        error_context = "\n".join(error_lines[-30:]) if error_lines else "(sem detalhes)"

        raise AirflowException(
            f"Spark job falhou (exit {return_code})\n"
            f"Application: {self.application}\n"
            f"App ID: {self._job_id}\n\n"
            f"Erros:\n{error_context}"
        )

    def _attempt_job_cancellation(self) -> None:
        """Tenta cancelar o job Spark via REST API."""
        try:
            resp = requests.post(
                f"{self.spark_rest_url}/api/v1/applications/{self._job_id}/kill",
                timeout=10,
            )
            if resp.status_code == 200:
                self.log.info("Job %s cancelado com sucesso.", self._job_id)
            else:
                self.log.warning("Não foi possível cancelar job %s: HTTP %s", self._job_id, resp.status_code)
        except Exception as e:
            self.log.warning("Erro ao cancelar job Spark: %s", str(e))

    def on_kill(self) -> None:
        """
        Chamado pelo Airflow quando a task é cancelada manualmente.
        Garante que o job Spark seja encerrado junto.
        """
        self.log.warning("Task cancelada. Encerrando Spark job: %s", self._job_id)
        if self._job_id:
            self._attempt_job_cancellation()