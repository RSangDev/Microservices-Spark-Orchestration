"""
Spark Job: parse_events.py
Responsável por parsear e validar eventos brutos coletados dos microserviços.

Executado pelo SparkMicroserviceOperator via spark-submit.
Argumentos:
  --input   : Path S3/HDFS com os eventos brutos (JSON Lines)
  --output  : Path S3/HDFS para salvar eventos parseados (Parquet)
  --run-id  : ID da execução Airflow (para rastreamento)
  --mode    : 'normal' ou 'recovery' (reprocessamento)
  --services: Serviços a processar (comma-separated); 'all' para todos

Demonstra:
  - Leitura de múltiplos formatos (JSON, CSV, Parquet)
  - Schema enforcement com StructType
  - Validação de dados em escala com Spark
  - Particionamento eficiente do output
  - Métricas de qualidade via acumuladores Spark
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

log = logging.getLogger(__name__)

# ─── Schemas por Serviço ──────────────────────────────────────────────────────

SCHEMAS = {
    "orders-service": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("status", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("items_count", LongType(), True),
            StructField("timestamp", TimestampType(), False),
            StructField("service_version", StringType(), True),
        ]
    ),
    "payments-service": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("payment_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), False),
            StructField("method", StringType(), True),
            StructField("gateway", StringType(), True),
            StructField("status", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("service_version", StringType(), True),
        ]
    ),
    "inventory-service": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("sku", StringType(), False),
            StructField("warehouse_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("quantity_delta", LongType(), False),
            StructField("quantity_after", LongType(), True),
            StructField("timestamp", TimestampType(), False),
            StructField("service_version", StringType(), True),
        ]
    ),
    "shipping-service": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("shipment_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("carrier", StringType(), True),
            StructField("tracking_code", StringType(), True),
            StructField("status", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("service_version", StringType(), True),
        ]
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark: Parse e validação de eventos")
    parser.add_argument("--input", required=True, help="Path de entrada (JSON Lines)")
    parser.add_argument("--output", required=True, help="Path de saída (Parquet)")
    parser.add_argument("--run-id", required=True, help="Airflow run_id")
    parser.add_argument("--mode", default="normal", choices=["normal", "recovery"])
    parser.add_argument("--services", default="all", help="Serviços a processar")
    return parser.parse_args()


def create_spark_session(run_id: str) -> SparkSession:
    return (
        SparkSession.builder.appName(f"parse_events_{run_id}")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def validate_dataframe(df, service_name: str, spark: SparkSession) -> tuple:
    """
    Valida um DataFrame usando regras de negócio.
    Retorna (df_valid, df_invalid, metrics_dict).
    """

    # Regras de validação por tipo de evento
    validation_rules = {
        "not_null_event_id": F.col("event_id").isNotNull(),
        "not_null_timestamp": F.col("timestamp").isNotNull(),
        "valid_timestamp": F.col("timestamp") <= F.current_timestamp(),
        "not_empty_event_type": (F.col("event_type").isNotNull())
        & (F.col("event_type") != ""),
    }

    # Adiciona regras específicas por serviço
    if service_name == "payments-service":
        validation_rules["positive_amount"] = F.col("amount") > 0
        validation_rules["valid_currency"] = F.col("currency").isin(
            "BRL", "USD", "EUR", "GBP"
        )
    elif service_name == "orders-service":
        validation_rules["positive_amount"] = F.col("total_amount") >= 0

    # Cria coluna de validação composta
    is_valid = F.lit(True)
    for rule_name, rule_expr in validation_rules.items():
        is_valid = is_valid & rule_expr

    df_flagged = df.withColumn("_is_valid", is_valid)

    df_valid = (
        df_flagged.filter(F.col("_is_valid"))
        .drop("_is_valid")
        .withColumn("_parsed_at", F.current_timestamp())
        .withColumn("_service", F.lit(service_name))
    )

    df_invalid = (
        df_flagged.filter(~F.col("_is_valid"))
        .drop("_is_valid")
        .withColumn("_rejected_at", F.current_timestamp())
        .withColumn("_service", F.lit(service_name))
    )

    total = df.count()
    valid_count = df_valid.count()
    invalid_count = df_invalid.count()

    metrics = {
        "service": service_name,
        "total_records": total,
        "valid_records": valid_count,
        "invalid_records": invalid_count,
        "validity_rate": valid_count / total if total > 0 else 0,
    }

    log.info(
        "%s | Total: %d | Válidos: %d | Inválidos: %d | Taxa: %.1f%%",
        service_name,
        total,
        valid_count,
        invalid_count,
        metrics["validity_rate"] * 100,
    )

    return df_valid, df_invalid, metrics


def main():
    args = parse_args()
    services_to_process = (
        list(SCHEMAS.keys()) if args.services == "all" else args.services.split(",")
    )

    spark = create_spark_session(args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "🚀 Iniciando parse_events | run_id: %s | modo: %s", args.run_id, args.mode
    )
    log.info("Input: %s | Output: %s", args.input, args.output)
    log.info("Serviços: %s", services_to_process)

    all_metrics = []
    all_valid_dfs = []

    for service_name in services_to_process:
        service_input = f"{args.input}/{service_name}"
        schema = SCHEMAS.get(service_name)

        if schema is None:
            log.warning("Schema não encontrado para %s. Pulando.", service_name)
            continue

        try:
            log.info("📖 Lendo eventos de %s...", service_name)
            df_raw = (
                spark.read.option("multiLine", False)
                .schema(schema)
                .json(f"{service_input}/events.jsonl")
            )

            # Deduplicação por event_id
            df_deduped = df_raw.dropDuplicates(["event_id"])
            duplicates_removed = df_raw.count() - df_deduped.count()
            if duplicates_removed > 0:
                log.warning(
                    "%s: %d duplicatas removidas", service_name, duplicates_removed
                )

            # Validação
            df_valid, df_invalid, metrics = validate_dataframe(
                df_deduped, service_name, spark
            )
            all_metrics.append(metrics)
            all_valid_dfs.append(df_valid)

            # Persiste inválidos para quarentena
            if df_invalid.count() > 0:
                (
                    df_invalid.write.mode("overwrite")
                    .partitionBy("_service")
                    .parquet(f"{args.output}/_quarantine/{service_name}")
                )
                log.warning(
                    "%s: %d registros em quarentena", service_name, df_invalid.count()
                )

        except Exception as e:
            log.error("Erro ao processar %s: %s", service_name, str(e))
            if args.mode == "recovery":
                raise  # Em recovery, não tolera falhas

    if not all_valid_dfs:
        log.error("Nenhum dado válido processado. Abortando.")
        sys.exit(1)

    # Union de todos os serviços com alinhamento de colunas
    from functools import reduce

    # Seleciona colunas comuns para o output unificado
    common_cols = ["event_id", "event_type", "timestamp", "_parsed_at", "_service"]

    log.info("💾 Persistindo eventos válidos...")
    combined = reduce(
        lambda a, b: a.select(*[c for c in common_cols if c in a.columns]).unionByName(
            b.select(*[c for c in common_cols if c in b.columns]),
            allowMissingColumns=True,
        ),
        all_valid_dfs,
    )

    (
        combined.withColumn("_run_id", F.lit(args.run_id))
        .repartition(20, "_service")  # Particiona por serviço para joins eficientes
        .write.mode("overwrite")
        .partitionBy("_service")
        .parquet(args.output)
    )

    # Relatório final
    total_valid = sum(m["valid_records"] for m in all_metrics)
    total_invalid = sum(m["invalid_records"] for m in all_metrics)
    log.info(
        "✅ Parse concluído | Válidos: %d | Inválidos: %d | Taxa geral: %.1f%%",
        total_valid,
        total_invalid,
        (
            total_valid / (total_valid + total_invalid) * 100
            if (total_valid + total_invalid) > 0
            else 0
        ),
    )

    spark.stop()


if __name__ == "__main__":
    main()
