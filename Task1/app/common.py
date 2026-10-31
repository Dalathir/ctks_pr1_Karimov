# имя очереди и подключение к RabbitMQ

import os
import pika

QUEUE = "well.repair.q"  # единственная очередь — суть "точка-точка"


def connect():
    """Подключение к брокеру. Адрес и логин берём из окружения Docker."""
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),  # в Docker — имя сервиса
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "app"),
            os.getenv("RABBITMQ_PASS", "app"),
        ),
    ))
