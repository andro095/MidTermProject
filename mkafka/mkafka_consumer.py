import json
from collections.abc import Callable
from typing import Generator

from kafka import KafkaConsumer


class MKafkaConsumer(KafkaConsumer):
    def __init__(self, host : str, port : str, topic : str, group_id : str = None, auto_offset_reset : str = 'latest', key_deserializer : Callable = None, value_deserializer : Callable = None, json_deserializer : bool = False, json_decoder: object = None, **kwargs):
        deserializer = value_deserializer
        if json_deserializer:
            deserializer = lambda v: json.loads(v, cls=json_decoder)

        super().__init__(
            topic,
            bootstrap_servers=f"{host}:{port}",
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            key_deserializer=key_deserializer,
            value_deserializer=deserializer,
            **kwargs
        )
        self.topic = topic

    def process[T](self) -> Generator[T]:
        for message in self:
            yield message.value

    def __del__(self):
        self.close()