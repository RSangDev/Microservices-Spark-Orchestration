"""
Plugin: ResultPublisherOperator
Publica resultados processados de volta para os microserviços via REST.
"""

from __future__ import annotations

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

log = logging.getLogger(__name__)


class ResultPublisherOperator(BaseOperator):
    """
    Lê resultados do data lake e publica via REST para os microserviços destino.

    :param service_configs: Lista de configs (service, endpoint, conn_id, metrics_path)
    :param results_path: Caminho base dos resultados no S3/local
    :param publish_timeout: Timeout por publicação (segundos)
    :param fail_on_partial: Se True, falha a task se algum serviço rejeitar os dados
    """

    template_fields = ("results_path",)
    ui_color = "#9C27B0"
    ui_fgcolor = "#ffffff"

    @apply_defaults
    def __init__(
        self,
        service_configs: list[dict],
        results_path: str,
        publish_timeout: int = 30,
        fail_on_partial: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.service_configs = service_configs
        self.results_path = results_path
        self.publish_timeout = publish_timeout
        self.fail_on_partial = fail_on_partial

    def execute(self, context: dict) -> dict:
        import requests

        results = {}
        for config in self.service_configs:
            service = config["service"]
            endpoint = config["endpoint"]

            try:
                # Em produção: lê parquet do S3, serializa para o formato do serviço
                payload = {"source": "airflow_pipeline", "run_id": context["run_id"]}

                resp = requests.post(endpoint, json=payload, timeout=self.publish_timeout)
                resp.raise_for_status()
                results[service] = {"status": "published", "http_status": resp.status_code}
                log.info("✅ Publicado para %s", service)

            except Exception as e:
                log.error("❌ Falha ao publicar para %s: %s", service, str(e))
                results[service] = {"status": "failed", "error": str(e)}

        failed = [s for s, r in results.items() if r["status"] == "failed"]
        if failed and self.fail_on_partial:
            raise RuntimeError(f"Publicação falhou para: {failed}")

        return results