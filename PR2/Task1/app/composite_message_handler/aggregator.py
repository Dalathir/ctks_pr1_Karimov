# Агрегатор: собирает расчёты по заявке и отправляет сводную смету

import json
import time

import pika

from ..common import QUEUE_AGGREGATOR, QUEUE_REPLY, connect, declare_topology

pending = {}  # order_id -> {"well_name", "expected", "works"}


def on_result(ch, method, properties, body):
    result = json.loads(body)
    order_id = result["order_id"]

    entry = pending.setdefault(order_id, {
        "well_name": result["well_name"],
        "expected": result["expected"],
        "works": {},
    })
    entry["works"][result["work"]] = result

    if len(entry["works"]) < entry["expected"]:
        ch.basic_ack(method.delivery_tag)
        return

    summary = {
        "order_id": order_id,
        "well_name": entry["well_name"],
        "total_cost": sum(r["cost"] for r in entry["works"].values()),
        "total_days": sum(r["days"] for r in entry["works"].values()),
        "works": entry["works"],
    }
    ch.basic_publish(
        exchange="",
        routing_key=QUEUE_REPLY,
        body=json.dumps(summary, ensure_ascii=False).encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"{time.strftime('%H:%M:%S')} "
          f"[aggregator] смета по {summary['well_name']}: "
          f"{summary['total_cost']} руб., {summary['total_days']} дн.", flush=True)

    del pending[order_id]
    ch.basic_ack(method.delivery_tag)


def main():
    channel = connect().channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_AGGREGATOR, on_message_callback=on_result)
    print(f"{time.strftime('%H:%M:%S')} [aggregator] ждёт расчёты...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
