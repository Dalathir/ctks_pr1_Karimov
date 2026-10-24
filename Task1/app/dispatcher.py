# Создает заявки

import json
import time
import uuid

import pika

from .common import QUEUE, connect


def main():
    channel = connect().channel()

    channel.queue_declare(queue=QUEUE, durable=True)

    for i in range(10):
        request = {"request_id": str(uuid.uuid4()), "well_id": f"W-{101 + i}"}

        channel.basic_publish(
            exchange="",            # default exchange: сразу в очередь
            routing_key=QUEUE,      # для default exchange это имя очереди
            body=json.dumps(request).encode(),  # тело сообщения — байты
            properties=pika.BasicProperties(delivery_mode=2),  # 2 = persistent
        )
        print(f"{time.strftime('%H:%M:%S')} "
              f"[диспетчер] заявка на {request['well_id']} отправлена", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
