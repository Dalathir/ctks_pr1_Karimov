# Исполнитель по бурению: считает стоимость и срок

import json
import random
import time

import pika

from ..common import EXCHANGE_RESULTS, WORK_QUEUES, connect, declare_topology

QUEUE = WORK_QUEUES["drilling"]
COST = 70000


def on_task(ch, method, properties, body):
    task = json.loads(body)
    print(f"{time.strftime('%H:%M:%S')} "
          f"[drilling] {task['well_name']} в работе", flush=True)
    time.sleep(3)  # имитация расчёта
    days = random.randint(5, 15)
    result = {
        "order_id": task["order_id"],
        "well_name": task["well_name"],
        "work": "drilling",
        "cost": COST * days,
        "days": days,
        "expected": task["expected"],
    }
    ch.basic_publish(
        exchange=EXCHANGE_RESULTS,
        routing_key="",  # fanout
        body=json.dumps(result, ensure_ascii=False).encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    ch.basic_ack(method.delivery_tag)


def main():
    channel = connect().channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_task)
    print(f"{time.strftime('%H:%M:%S')} [drilling] ждёт работы...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
