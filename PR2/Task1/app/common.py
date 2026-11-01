# Общие настройки задания 2: брокер и топология

import os

import pika

EXCHANGE_IN = "repair.in"
EXCHANGE_SPLIT = "repair.split"
EXCHANGE_WORK = "repair.work"
EXCHANGE_RESULTS = "repair.results"

QUEUE_SPLITTER = "splitter.q"
QUEUE_ROUTER = "router.q"
QUEUE_AGGREGATOR = "aggregator.q"
QUEUE_REPLY = "reply.q"

# вид работ -> очередь исполнителя
WORK_QUEUES = {
    "drilling": "work.drilling.q",
    "electrical": "work.electrical.q",
    "logistics": "work.logistics.q",
}


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "app"),
            os.getenv("RABBITMQ_PASS", "app"),
        ),
    ))


def declare_topology(channel):
    # обменники
    channel.exchange_declare(exchange=EXCHANGE_IN, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_SPLIT, exchange_type="fanout", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_WORK, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_RESULTS, exchange_type="fanout", durable=True)

    # разделитель
    channel.queue_declare(queue=QUEUE_SPLITTER, durable=True)
    channel.queue_bind(queue=QUEUE_SPLITTER, exchange=EXCHANGE_IN, routing_key="repair.request")

    # маршрутизатор
    channel.queue_declare(queue=QUEUE_ROUTER, durable=True)
    channel.queue_bind(queue=QUEUE_ROUTER, exchange=EXCHANGE_SPLIT)

    # исполнители
    for work, queue in WORK_QUEUES.items():
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(queue=queue, exchange=EXCHANGE_WORK, routing_key=f"work.{work}")

    # агрегатор и ответ инициатору
    channel.queue_declare(queue=QUEUE_AGGREGATOR, durable=True)
    channel.queue_bind(queue=QUEUE_AGGREGATOR, exchange=EXCHANGE_RESULTS)
    channel.queue_declare(queue=QUEUE_REPLY, durable=True)
