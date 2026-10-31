# Отдел: слушает свою очередь и получает копию каждого события

import json
import os
import time

from .common import EXCHANGE, connect, declare_exchange

NAME = os.getenv("DEPT_NAME", "отдел")      # Бухгалтерия / Нефтебаза / Аналитика
QUEUE = os.getenv("QUEUE", "price.default") # у каждого отдела своя очередь


def main():
    channel = connect().channel()

    # Своя очередь + привязка к общему fanout. Копию получит каждый отдел.
    declare_exchange(channel)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE)

    def on_message(ch, method, properties, body):
        event = json.loads(body)
        print(
            f"{time.strftime('%H:%M:%S')} "
            f"[{NAME}] {event['product']}: "
            f"{event['old_price']} -> {event['new_price']}",
            flush=True,
        )
        ch.basic_ack(method.delivery_tag)  # подтверждаем обработку

    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
    print(f"{time.strftime('%H:%M:%S')} [{NAME}] ждёт новости...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
