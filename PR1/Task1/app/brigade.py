# Бригада: забирает заявки из общей очереди и подтверждает обработку.

import json
import os
import time
from .common import QUEUE, connect

NAME = os.getenv("BRIGADE_NAME", "brigade")  # имя из docker-compose


def main():
    channel = connect().channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)  # не больше одной заявки за раз

    def on_message(ch, method, properties, body):
        well_id = json.loads(body)["well_id"]
        print(f" {time.strftime('%H:%M:%S')} "
              f"[{NAME}] взяла заявку на {well_id}", flush=True)
        time.sleep(7)  # имитация ремонта
        ch.basic_ack(method.delivery_tag)  # подтверждаем — сообщение удалится
        print(f" {time.strftime('%H:%M:%S')} "
              f"[{NAME}] закончила {well_id}", flush=True)

    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
    print(f"{time.strftime('%H:%M:%S')} "
          f"[{NAME}] ждёт заявки...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
