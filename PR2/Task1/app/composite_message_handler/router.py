# Маршрутизатор: отправляет работу в очередь нужного исполнителя

import json
import time

import pika

from ..common import EXCHANGE_WORK, QUEUE_ROUTER, connect, declare_topology


def on_task(ch, method, properties, body):
    task = json.loads(body)
    work = task["work"]

    ch.basic_publish(
        exchange=EXCHANGE_WORK,
        routing_key=f"work.{work}",
        body=json.dumps(task, ensure_ascii=False).encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"{time.strftime('%H:%M:%S')} "
          f"[router] {task['well_name']} -> work.{work}", flush=True)
    ch.basic_ack(method.delivery_tag)


def main():
    channel = connect().channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_ROUTER, on_message_callback=on_task)
    print(f"{time.strftime('%H:%M:%S')} [router] ждёт работы...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
