# Издатель: публикует изменение цены и завершается

import json
import time
import uuid

import pika

from .common import EXCHANGE, QUEUES, connect, declare_exchange


def main():
    channel = connect().channel()

    # Обменник + очереди + привязки. Создаём до публикации, чтобы не потерять сообщение.
    declare_exchange(channel)
    for queue in QUEUES:
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(queue=queue, exchange=EXCHANGE)  # привязка к fanout

    prices = [78.4, 79.1, 75.5, 70.9]

    for i in range(1, len(prices)):
        event = {
            "event_id": str(uuid.uuid4()),
            "product": "Brent",
            "old_price": prices[i-1],
            "new_price": prices[i],
        }
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key="",  # для fanout routing key игнорируется
            body=json.dumps(event, ensure_ascii=False).encode(),  # байты
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )

        print(
            f"{time.strftime('%H:%M:%S')} "
            f"[издатель] {event['product']}: "
            f"{event['old_price']} -> {event['new_price']}",
            flush=True,
        )
        time.sleep(10)


if __name__ == "__main__":
    main()
