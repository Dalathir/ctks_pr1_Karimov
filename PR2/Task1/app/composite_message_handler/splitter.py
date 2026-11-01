# Разделитель: режет комплексную заявку на отдельные виды работ

import json
import time

import pika

from ..common import EXCHANGE_SPLIT, QUEUE_SPLITTER, WORK_QUEUES, connect, declare_topology


def on_order(ch, method, properties, body):
    order = json.loads(body)
    works = [work for work in WORK_QUEUES if order.get(work)]

    for work in works:
        task = {
            "order_id": order["order_id"],
            "well_name": order["well_name"],
            "work": work,
            "expected": len(works),  # сколько расчётов ждать агрегатору
        }
        ch.basic_publish(
            exchange=EXCHANGE_SPLIT,
            routing_key="",  # fanout ключ не учитывает
            body=json.dumps(task, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"{time.strftime('%H:%M:%S')} "
              f"[splitter] {order['well_name']}: {work}", flush=True)

    ch.basic_ack(method.delivery_tag)


def main():
    channel = connect().channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_SPLITTER, on_message_callback=on_order)
    print(f"{time.strftime('%H:%M:%S')} [splitter] ждёт заявки...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
