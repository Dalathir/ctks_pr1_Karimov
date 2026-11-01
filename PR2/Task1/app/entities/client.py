# Клиент: создает заявку и ожидает смету
import json
import time
import uuid
import pika
from ..common import EXCHANGE_IN, QUEUE_REPLY, connect, declare_topology

ORDERS = [
    {
        'order_id' : str(uuid.uuid4()),
        'well_name': 'W-101',
        'drilling' : True,
        'electrical' : True,
        'logistics' : True
    },
    {
        'order_id' : str(uuid.uuid4()),
        'well_name': 'W-102',
        'drilling' : True,
        'electrical' : False,
        'logistics' : True
    },
    {
        'order_id' : str(uuid.uuid4()),
        'well_name': 'W-103',
        'drilling' : False,
        'electrical' : True,
        'logistics' : True
    },
]


def on_message(ch, method, properties, body):
    message = json.loads(body)
    print(f"{time.strftime('%H:%M:%S')} "
          f"[клиент] смета по {message['well_name']}: {message}", flush=True)
    ch.basic_ack(method.delivery_tag)  # подтверждаем — иначе сообщение вернётся в очередь


def main():
    channel = connect().channel()
    declare_topology(channel)

    for order in ORDERS:
        channel.basic_publish(
            exchange=EXCHANGE_IN,
            routing_key="repair.request",
            body=json.dumps(order, ensure_ascii=False).encode(),  # байты
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        print(f"{time.strftime('%H:%M:%S')} "
              f"[клиент] заявка на {order['well_name']} отправлена {order}", flush=True)
        time.sleep(3)


    # после отправки — начинаем принимать сметы
    channel.basic_consume(queue=QUEUE_REPLY, on_message_callback=on_message)
    print(f"{time.strftime('%H:%M:%S')} [клиент] ждёт сводные сметы...", flush=True)
    channel.start_consuming()



if __name__ == "__main__":
    main()