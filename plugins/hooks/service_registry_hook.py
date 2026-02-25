"""
Plugin: ServiceRegistryHook
Hook para consultar um Service Registry (Consul/Eureka/custom) e
obter lista dinâmica de microserviços registrados.
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


class ServiceRegistryHook(BaseHook):
    """
    Integra com um Service Registry para descoberta dinâmica de serviços.
    Suporta: Consul, Eureka, ou endpoint REST customizado.

    :param conn_id: ID da conexão Airflow com a URL do registry
    """

    conn_name_attr = "conn_id"
    default_conn_name = "service_registry"
    conn_type = "http"
    hook_name = "Service Registry"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = conn_id
        self._base_url: str | None = None

    def get_conn(self) -> str:
        conn = self.get_connection(self.conn_id)
        self._base_url = f"http://{conn.host}:{conn.port}"
        return self._base_url

    def list_active_services(self, tag: str | None = None) -> list[dict]:
        """
        Lista todos os serviços ativos no registry, opcionalmente filtrados por tag.

        :param tag: Filtra serviços com uma tag específica (ex.: 'event-producer')
        :return: Lista de configs de serviço no formato esperado pelas DAGs
        """
        import requests

        base_url = self.get_conn()
        params = {"tag": tag} if tag else {}

        resp = requests.get(f"{base_url}/v1/catalog/services", params=params, timeout=10)
        resp.raise_for_status()
        raw_services = resp.json()

        services = []
        for service_name, service_tags in raw_services.items():
            if tag and tag not in service_tags:
                continue

            detail_resp = requests.get(
                f"{base_url}/v1/catalog/service/{service_name}",
                timeout=10,
            )
            detail_resp.raise_for_status()
            details = detail_resp.json()

            if details:
                svc = details[0]
                services.append({
                    "name": service_name,
                    "base_url": f"http://{svc['ServiceAddress']}:{svc['ServicePort']}",
                    "events_endpoint": svc.get("ServiceMeta", {}).get("events_endpoint", "/api/v1/events/export"),
                    "conn_id": f"{service_name.replace('-', '_')}",
                    "topic": svc.get("ServiceMeta", {}).get("kafka_topic", f"{service_name}.events"),
                    "priority": svc.get("ServiceMeta", {}).get("priority", "medium"),
                })

        log.info("Serviços encontrados no registry (tag='%s'): %d", tag, len(services))
        return services

    def register_pipeline_run(self, dag_id: str, run_id: str, metadata: dict) -> bool:
        """
        Registra uma execução do pipeline no registry para rastreamento.
        Permite que os microserviços saibam que o pipeline está rodando.
        """
        import requests

        base_url = self.get_conn()
        payload = {
            "dag_id": dag_id,
            "run_id": run_id,
            "status": "running",
            **metadata,
        }

        try:
            resp = requests.put(
                f"{base_url}/v1/kv/airflow/pipelines/{dag_id}/{run_id}",
                json=payload,
                timeout=10,
            )
            return resp.status_code == 200
        except Exception as e:
            log.warning("Não foi possível registrar run no registry: %s", str(e))
            return False