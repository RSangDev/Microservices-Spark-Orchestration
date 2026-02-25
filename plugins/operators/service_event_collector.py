"""
Plugin: ServiceEventCollectorOperator
Coleta eventos de múltiplos microserviços e persiste no data lake (S3/local).
Suporta modo full (todos os serviços) e partial (apenas serviços críticos/high).
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class ServiceEventCollectorOperator(BaseOperator):
    """
    Coleta eventos de múltiplos microserviços em paralelo e salva no data lake.

    :param services: Lista de configs de serviços (name, base_url, events_endpoint, etc.)
    :param output_path: Caminho base para salvar eventos (S3 ou local)
    :param mode: 'full' (todos os serviços) ou 'partial' (apenas críticos/high)
    :param batch_size: Tamanho do batch para paginação da API
    :param compression: Compressão dos arquivos ('snappy', 'gzip', 'none')
    :param max_workers: Threads paralelas para coleta
    """

    template_fields = ("output_path",)
    ui_color = "#2196F3"
    ui_fgcolor = "#ffffff"

    @apply_defaults
    def __init__(
        self,
        services: list[dict],
        output_path: str,
        mode: str = "full",
        batch_size: int = 10_000,
        compression: str = "snappy",
        max_workers: int = 4,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.services = services
        self.output_path = output_path
        self.mode = mode
        self.batch_size = batch_size
        self.compression = compression
        self.max_workers = max_workers

    def execute(self, context: dict) -> dict:
        ds = context["ds"]
        ts = context["ts"]

        self.log.info(
            "📥 Coletando eventos | Modo: %s | Serviços: %d | Output: %s",
            self.mode, len(self.services), self.output_path,
        )

        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._collect_from_service, svc, ds, ts): svc["name"]
                for svc in self.services
            }

            for future in as_completed(futures):
                service_name = futures[future]
                try:
                    result = future.result()
                    results[service_name] = result
                    self.log.info(
                        "✅ %s — %d eventos coletados em %.2fs",
                        service_name, result["event_count"], result["duration_seconds"]
                    )
                except Exception as e:
                    self.log.error("❌ Falha ao coletar de %s: %s", service_name, str(e))
                    results[service_name] = {"status": "failed", "error": str(e)}

        summary = {
            "mode": self.mode,
            "output_path": self.output_path,
            "services_collected": len([r for r in results.values() if r.get("status") != "failed"]),
            "services_failed": len([r for r in results.values() if r.get("status") == "failed"]),
            "total_events": sum(r.get("event_count", 0) for r in results.values()),
            "details": results,
        }

        self.log.info(
            "📦 Coleta concluída: %d eventos de %d/%d serviços",
            summary["total_events"],
            summary["services_collected"],
            len(self.services),
        )
        return summary

    def _collect_from_service(self, service: dict, ds: str, ts: str) -> dict:
        """Coleta todos os eventos de um serviço com paginação."""
        import time
        start = time.time()

        name = service["name"]
        base_url = service["base_url"]
        endpoint = service.get("events_endpoint", "/api/v1/events/export")
        conn_id = service.get("conn_id")

        all_events = []
        page = 1

        while True:
            resp = requests.get(
                f"{base_url}{endpoint}",
                params={
                    "date": ds,
                    "page": page,
                    "limit": self.batch_size,
                    "format": "json",
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            events = data.get("events", data.get("items", []))
            all_events.extend(events)

            self.log.debug("%s — página %d: %d eventos", name, page, len(events))

            if not data.get("has_next", False) or len(events) < self.batch_size:
                break
            page += 1

        # Persiste no data lake
        output_file = self._persist_events(name, all_events, ds)

        return {
            "status": "success",
            "service": name,
            "event_count": len(all_events),
            "pages_fetched": page,
            "output_file": output_file,
            "duration_seconds": round(time.time() - start, 2),
        }

    def _persist_events(self, service_name: str, events: list[dict], ds: str) -> str:
        """
        Persiste eventos no formato JSON Lines (ou Parquet em produção).
        Em produção: usar pyarrow + boto3 para escrever Parquet no S3.
        """
        import os

        output_dir = f"{self.output_path}/{service_name}"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/events.jsonl"

        with open(output_file, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")

        self.log.debug("Persistido: %s (%d eventos)", output_file, len(events))
        return output_file