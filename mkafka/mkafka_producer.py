import json
from collections.abc import Callable

from kafka import KafkaProducer


class MKafkaProducer(KafkaProducer):
    def __init__(self, host : str, port : str, topic : str, compression_type : str = None, key_serializer : Callable = None, value_serializer : Callable = None, json_serializer : bool = False, json_encoder : object = None, **kwargs):
        serializer = value_serializer
        if json_serializer and json_encoder:
            serializer = lambda v: json.dumps(v, cls=json_encoder).encode()

        super().__init__(
            bootstrap_servers=f"{host}:{port}",
            compression_type=compression_type,
            key_serializer=key_serializer,
            value_serializer=serializer,
            **kwargs
        )
        self.topic = topic

    def produce(self, message, **kwargs):
        self.send(self.topic, message, **kwargs)
        self.flush()

    def change_topic(self, topic):
        self.topic = topic

    def __del__(self):
        self.close()