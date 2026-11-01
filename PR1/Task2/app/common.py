# имя очереди и подключение к RabbitMQ

import os
import pika

EXCHANGE = "price.fanout"  # обменник типа fanout — рассылает копию всем

# Очереди отделов. Издатель объявляет их сам, чтобы сообщение не потерялось,
# если отдел ещё не успел подключиться к брокеру.
QUEUES = [
    "price.accounting",
    "price.oildepot",
    "price.analytics",
]


def connect():
    """Подключение к брокеру. Адрес и логин берём из окружения Docker."""
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),  # в Docker — имя сервиса
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "app"),
            os.getenv("RABBITMQ_PASS", "app"),
        ),
    ))


def declare_exchange(channel):
    """Объявляет обменник."""
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
