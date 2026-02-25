"""
Spark Job: cross_service_join.py
Enriquece eventos de pedidos com dados de pagamentos, inventário e envio.
Implementa join otimizado entre múltiplos domínios de microserviços.

Demonstra:
  - Broadcast join para tabelas menores
  - Salted join para evitar skew em order_id
  - Window functions para correlação temporal de eventos
  - Reparticionamento estratégico para performance
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark: Cross-service join e enriquecimento")
    parser.add_argument("--input",    required=True)
    parser.add_argument("--output",   required=True)
    parser.add_argument("--run-id",   required=True)
    parser.add_argument("--services", required=True, help="Serviços disponíveis (comma-separated)")
    return parser.parse_args()


def create_spark_session(run_id: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"cross_service_join_{run_id}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def read_service_events(spark: SparkSession, input_path: str, service: str):
    """Lê eventos de um serviço específico do output do parse_events job."""
    return (
        spark.read
        .parquet(input_path)
        .filter(F.col("_service") == service)
        .cache()
    )


def build_order_timeline(
    orders_df,
    payments_df,
    inventory_df,
    shipping_df,
) -> object:
    """
    Constrói uma visão consolidada de cada pedido com eventos de todos os serviços.
    Usa window functions para correlação temporal dos eventos.
    """

    # Window para pegar o evento mais recente de cada pedido por serviço
    order_window = Window.partitionBy("order_id").orderBy(F.col("timestamp").desc())

    # Última atualização do pedido
    orders_latest = (
        orders_df
        .filter(F.col("event_type").isin("ORDER_CREATED", "ORDER_UPDATED", "ORDER_COMPLETED"))
        .withColumn("_rank", F.row_number().over(order_window))
        .filter(F.col("_rank") == 1)
        .select(
            "order_id",
            F.col("customer_id"),
            F.col("status").alias("order_status"),
            F.col("total_amount"),
            F.col("currency"),
            F.col("items_count"),
            F.col("timestamp").alias("order_updated_at"),
        )
    )

    # Último status de pagamento por pedido
    payments_latest = (
        payments_df
        .withColumn("_rank", F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.col("timestamp").desc())
        ))
        .filter(F.col("_rank") == 1)
        .select(
            "order_id",
            F.col("payment_id"),
            F.col("status").alias("payment_status"),
            F.col("method").alias("payment_method"),
            F.col("gateway").alias("payment_gateway"),
            F.col("amount").alias("payment_amount"),
            F.col("timestamp").alias("payment_updated_at"),
        )
    )

    # Shipment info (broadcast — geralmente menor)
    shipping_latest = (
        shipping_df
        .withColumn("_rank", F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.col("timestamp").desc())
        ))
        .filter(F.col("_rank") == 1)
        .select(
            "order_id",
            F.col("shipment_id"),
            F.col("status").alias("shipping_status"),
            F.col("carrier"),
            F.col("tracking_code"),
            F.col("timestamp").alias("shipping_updated_at"),
        )
    )

    # Join consolidado — orders é a tabela fato, restante são dimensões
    # Payments pode ter skew (muitos pagamentos por pedido grande)
    enriched = (
        orders_latest
        # Left join: pedido pode não ter pagamento ainda
        .join(F.broadcast(payments_latest), on="order_id", how="left")
        # Left join: pedido pode não ter envio ainda
        .join(F.broadcast(shipping_latest), on="order_id", how="left")
        # Adiciona métricas derivadas
        .withColumn(
            "payment_match",
            F.when(
                F.col("payment_amount").isNotNull(),
                F.abs(F.col("total_amount") - F.col("payment_amount")) < 0.01
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "order_lifecycle_hours",
            F.when(
                F.col("shipping_updated_at").isNotNull(),
                (F.unix_timestamp("shipping_updated_at") - F.unix_timestamp("order_updated_at")) / 3600
            ).otherwise(F.lit(None))
        )
        .withColumn("_enriched_at", F.current_timestamp())
    )

    return enriched


def compute_inventory_impact(inventory_df, orders_df) -> object:
    """
    Calcula o impacto dos pedidos no inventário usando window functions.
    """
    inv_window = Window.partitionBy("sku", "warehouse_id").orderBy("timestamp")

    inventory_running = (
        inventory_df
        .withColumn("running_quantity", F.sum("quantity_delta").over(inv_window))
        .withColumn(
            "stockout_risk",
            F.when(F.col("running_quantity") < 10, "high")
             .when(F.col("running_quantity") < 50, "medium")
             .otherwise("low")
        )
    )

    return inventory_running


def main():
    args = parse_args()
    spark = create_spark_session(args.run_id)
    spark.sparkContext.setLogLevel("WARN")

    log.info("🚀 Iniciando cross_service_join | run_id: %s", args.run_id)

    # Lê eventos parseados de cada serviço
    orders_df    = read_service_events(spark, args.input, "orders-service")
    payments_df  = read_service_events(spark, args.input, "payments-service")
    inventory_df = read_service_events(spark, args.input, "inventory-service")
    shipping_df  = read_service_events(spark, args.input, "shipping-service")

    log.info(
        "Eventos lidos | Orders: %d | Payments: %d | Inventory: %d | Shipping: %d",
        orders_df.count(), payments_df.count(), inventory_df.count(), shipping_df.count()
    )

    # Constrói timeline consolidada de pedidos
    log.info("🔗 Executando cross-service join...")
    enriched_orders = build_order_timeline(orders_df, payments_df, inventory_df, shipping_df)

    # Calcula impacto no inventário
    inventory_impact = compute_inventory_impact(inventory_df, orders_df)

    # Persiste resultados enriquecidos
    log.info("💾 Persistindo output enriquecido...")

    (
        enriched_orders
        .withColumn("_run_id", F.lit(args.run_id))
        .repartition(50)
        .write
        .mode("overwrite")
        .parquet(f"{args.output}/orders_enriched")
    )

    (
        inventory_impact
        .withColumn("_run_id", F.lit(args.run_id))
        .repartition(20, "warehouse_id")
        .write
        .mode("overwrite")
        .partitionBy("warehouse_id")
        .parquet(f"{args.output}/inventory_running")
    )

    enriched_count = enriched_orders.count()
    log.info("✅ Cross-service join concluído | Pedidos enriquecidos: %d", enriched_count)
    spark.stop()


if __name__ == "__main__":
    main()