"""
mock_server.py
Servidor Flask que simula todos os 4 microserviços em portas diferentes.
Usado apenas em desenvolvimento local via docker-compose --profile mocks.
"""

import os
import random
import time

from flask import Flask, jsonify

PORT = int(os.environ.get("SERVICE_PORT", 8001))
SERVICE_NAME = os.environ.get("SERVICE_NAME", "orders-service")
SERVICE_VERSION = os.environ.get("SERVICE_VERSION", "1.0.0")

app = Flask(__name__)

# ── Health ─────────────────────────────────────────────────────────────────────


@app.route("/health")
def health():
    return jsonify(
        {
            "status": "healthy",
            "service": SERVICE_NAME,
            "version": SERVICE_VERSION,
            "uptime": int(time.time()),
            "dependencies": {
                "database": {"status": "healthy"},
                "cache": {"status": "healthy"},
            },
        }
    )


# ── Eventos ────────────────────────────────────────────────────────────────────


@app.route("/api/v1/events/export")
def events():
    service_events = {
        "orders-service": [
            {
                "event_id": f"evt_{i}",
                "order_id": f"ord_{i}",
                "customer_id": f"cust_{i % 100}",
                "event_type": "ORDER_CREATED",
                "status": "pending",
                "total_amount": round(random.uniform(10, 500), 2),
                "currency": "BRL",
                "items_count": random.randint(1, 10),
                "timestamp": "2024-01-15T10:00:00Z",
                "service_version": SERVICE_VERSION,
            }
            for i in range(50)
        ],
        "payments-service": [
            {
                "event_id": f"pay_evt_{i}",
                "payment_id": f"pay_{i}",
                "order_id": f"ord_{i}",
                "event_type": "PAYMENT_PROCESSED",
                "amount": round(random.uniform(10, 500), 2),
                "currency": "BRL",
                "method": random.choice(["credit_card", "pix", "boleto"]),
                "gateway": "stripe",
                "status": "approved",
                "timestamp": "2024-01-15T10:01:00Z",
                "service_version": SERVICE_VERSION,
            }
            for i in range(50)
        ],
        "inventory-service": [
            {
                "event_id": f"inv_evt_{i}",
                "sku": f"SKU-{i % 20:04d}",
                "warehouse_id": f"WH-{i % 3 + 1}",
                "event_type": "STOCK_UPDATED",
                "quantity_delta": random.randint(-10, 50),
                "quantity_after": random.randint(0, 200),
                "timestamp": "2024-01-15T10:00:30Z",
                "service_version": SERVICE_VERSION,
            }
            for i in range(50)
        ],
        "shipping-service": [
            {
                "event_id": f"ship_evt_{i}",
                "shipment_id": f"ship_{i}",
                "order_id": f"ord_{i}",
                "event_type": "SHIPMENT_DISPATCHED",
                "carrier": random.choice(["correios", "fedex", "jadlog"]),
                "tracking_code": f"TR{i:010d}BR",
                "status": "in_transit",
                "timestamp": "2024-01-15T11:00:00Z",
                "service_version": SERVICE_VERSION,
            }
            for i in range(50)
        ],
    }

    events_list = service_events.get(SERVICE_NAME, [])
    return jsonify(
        {"events": events_list, "has_next": False, "total": len(events_list)}
    )


# ── Ingestão de métricas (publisher) ──────────────────────────────────────────


@app.route("/api/v1/metrics/ingest", methods=["POST"])
def ingest():
    return jsonify({"status": "accepted", "records": 1250})


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"[{SERVICE_NAME}] iniciando na porta {PORT} (v{SERVICE_VERSION})")
    app.run(host="0.0.0.0", port=PORT)
