"""
Spark Job: aggregate_metrics.py
Computa métricas de negócio agregadas por janela temporal.
Alimenta dashboards e os próprios microserviços com KPIs calculados.

Demonstra:
  - Aggregações com groupBy e pivot
  - Window functions para séries temporais
  - Percentis e estatísticas avançadas
  - Output multi-formato (Parquet + JSON summary)
"""

import argparse
import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--window", default="30m", help="Janela de agregação (ex.: 30m, 1h)")
    parser.add_argument("--mode",   default="normal", choices=["normal", "recovery"])
    return parser.parse_args()


def create_spark_session(run_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"aggregate_metrics_{run_id}")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def compute_order_metrics(enriched_df) -> dict:
    """Métricas de pedidos: volume, ticket médio, conversão por status."""

    order_metrics = (
        enriched_df
        .groupBy(
            F.window("order_updated_at", "30 minutes").alias("time_window"),
            "order_status",
            "currency",
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("gross_revenue"),
            F.avg("total_amount").alias("avg_ticket"),
            F.expr("percentile_approx(total_amount, 0.5)").alias("median_ticket"),
            F.expr("percentile_approx(total_amount, 0.95)").alias("p95_ticket"),
            F.sum("items_count").alias("total_items"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end",   F.col("time_window.end"))
        .drop("time_window")
    )

    return order_metrics


def compute_payment_funnel(enriched_df) -> object:
    """
    Funil de pagamento: taxa de aprovação, métodos mais usados, fraude.
    """
    payment_funnel = (
        enriched_df
        .filter(F.col("payment_status").isNotNull())
        .groupBy("payment_method", "payment_gateway", "payment_status")
        .agg(
            F.count("order_id").alias("transaction_count"),
            F.sum("payment_amount").alias("total_amount"),
            F.avg("payment_amount").alias("avg_amount"),
        )
        .withColumn(
            "approval_rate",
            F.when(
                F.col("payment_status") == "approved",
                F.lit(1.0)
            ).otherwise(F.lit(0.0))
        )
    )

    return payment_funnel


def compute_sla_metrics(enriched_df) -> object:
    """
    SLA de fulfillment: tempo entre criação do pedido e envio.
    Classifica pedidos dentro e fora do SLA.
    """
    sla_threshold_hours = 24.0

    sla_metrics = (
        enriched_df
        .filter(F.col("order_lifecycle_hours").isNotNull())
        .withColumn(
            "sla_status",
            F.when(F.col("order_lifecycle_hours") <= sla_threshold_hours, "within_sla")
             .otherwise("sla_breached")
        )
        .groupBy("sla_status", "currency")
        .agg(
            F.count("order_id").alias("order_count"),
            F.avg("order_lifecycle_hours").alias("avg_fulfillment_hours"),
            F.expr("percentile_approx(order_lifecycle_hours, 0.95)").alias("p95_fulfillment_hours"),
            F.sum("total_amount").alias("affected_revenue"),
        )
    )

    return sla_metrics


def main():
    args = parse_args()
    spark = create_spark_session(args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    log.info("🚀 Iniciando aggregate_metrics | run_id: %s | window: %s", args.run_id, args.window)

    # Lê dados enriquecidos do cross_service_join
    enriched = spark.read.parquet(f"{args.input}/orders_enriched").cache()
    total_records = enriched.count()
    log.info("Registros lidos: %d", total_records)

    # Computa métricas
    log.info("📊 Computando métricas de pedidos...")
    order_metrics = compute_order_metrics(enriched)

    log.info("💳 Computando funil de pagamento...")
    payment_funnel = compute_payment_funnel(enriched)

    log.info("⏱️  Computando métricas de SLA...")
    sla_metrics = compute_sla_metrics(enriched)

    # Persiste resultados
    for name, df in [
        ("order_metrics", order_metrics),
        ("payment_funnel", payment_funnel),
        ("sla_metrics", sla_metrics),
    ]:
        (
            df
            .withColumn("_run_id", F.lit(args.run_id))
            .write
            .mode("overwrite")
            .parquet(f"{args.output}/{name}")
        )
        log.info("✅ %s persistido", name)

    # Resumo JSON para observabilidade rápida
    summary = {
        "run_id": args.run_id,
        "total_records": total_records,
        "metrics_computed": ["order_metrics", "payment_funnel", "sla_metrics"],
    }
    log.info("📋 Resumo: %s", json.dumps(summary))

    spark.stop()


if __name__ == "__main__":
    main()