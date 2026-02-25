"""
Plugin: MicroserviceHealthSensor
Sensor que aguarda um microserviço atingir um estado de saúde esperado.
Suporta: healthy, degraded, starting. Modo reschedule para não bloquear workers.
"""

from __future__ import annotations

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class MicroserviceHealthSensor(BaseSensorOperator):
    """
    Aguarda que um microserviço atinja o status de health esperado.

    :param service_url: URL do endpoint /health do serviço
    :param expected_status: Status esperado ('healthy', 'degraded', 'starting')
    :param soft_fail: Se True, marca como skipped em vez de falhar no timeout
    :param required_dependencies: Dependências que devem estar healthy (opcional)
    """

    template_fields = ("service_url",)
    ui_color = "#00BCD4"
    ui_fgcolor = "#ffffff"

    @apply_defaults
    def __init__(
        self,
        service_url: str,
        expected_status: str = "healthy",
        soft_fail: bool = False,
        required_dependencies: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(soft_fail=soft_fail, **kwargs)
        self.service_url = service_url
        self.expected_status = expected_status
        self.required_dependencies = required_dependencies or []

    def poke(self, context: dict) -> bool:
        import requests

        self.log.info(
            "🔍 Verificando saúde de: %s (esperado: %s)",
            self.service_url,
            self.expected_status,
        )

        try:
            resp = requests.get(self.service_url, timeout=10)

            if resp.status_code == 200:
                health = resp.json()
                status = health.get("status", "unknown")

                # Verifica dependências internas do serviço
                if self.required_dependencies:
                    deps = health.get("dependencies", {})
                    unhealthy_deps = [
                        d
                        for d in self.required_dependencies
                        if deps.get(d, {}).get("status") != "healthy"
                    ]
                    if unhealthy_deps:
                        self.log.warning(
                            "Dependências ainda não saudáveis: %s", unhealthy_deps
                        )
                        return False

                if status == self.expected_status:
                    self.log.info(
                        "✅ Serviço saudável: %s (v%s)",
                        self.service_url,
                        health.get("version", "?"),
                    )
                    return True

                self.log.info(
                    "Aguardando... Status atual: '%s', esperado: '%s'",
                    status,
                    self.expected_status,
                )
                return False

            elif resp.status_code in (503, 502):
                self.log.info(
                    "Serviço ainda iniciando (HTTP %d). Aguardando...", resp.status_code
                )
                return False
            else:
                self.log.warning("Resposta inesperada: HTTP %d", resp.status_code)
                return False

        except requests.exceptions.ConnectionError:
            self.log.info("Serviço inacessível. Aguardando reconexão...")
            return False
        except requests.exceptions.Timeout:
            self.log.warning("Timeout ao verificar %s", self.service_url)
            return False
        except Exception as e:
            self.log.error("Erro inesperado no health check: %s", str(e))
            return False
