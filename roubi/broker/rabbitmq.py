from pika import BlockingConnection
from pika.credentials import PlainCredentials
from pika.connection import ConnectionParameters
from typica import BaseConnection, ConnectionMeta, ConnectionUriMeta


class BrokerRepository(BaseConnection):
    def __init__(self, metadata: ConnectionMeta | ConnectionUriMeta, **kwargs) -> None:
        super().__init__(metadata)
        try:
            self.client = BlockingConnection(
                ConnectionParameters(
                    host=self._metadata.host,
                    port=self._metadata.port,
                    virtual_host=self._metadata.database,
                    credentials=PlainCredentials(
                        username=self._metadata.username,
                        password=self._metadata.password,
                    ),
                    heartbeat=60,
                    blocked_connection_timeout=360,
                    **kwargs
                )
            )
            self.channel = self.client.channel()
        except Exception:
            raise

    def close(self) -> None:
        self.client.close()

    def declare_exchange(self, exchange: str, **kwargs) -> None:
        try:
            if self.client == False:
                raise ValueError("Fail connect")
            self.channel.exchange_declare(exchange=exchange, **kwargs)
        except Exception:
            raise

    def declare_queue(
        self, queue: str, exchange: str, routing_key: str = None, **kwargs
    ) -> None:
        try:
            if self.client == False:
                raise ValueError("Fail connect")
            self.channel.queue_declare(queue=queue, **kwargs)
            self.channel.queue_bind(
                queue=queue, exchange=exchange, routing_key=routing_key
            )
        except Exception:
            raise

    def send_message(self, exchange: str, routing_key: str, message: str) -> None:
        try:
            if self.client == False:
                raise ValueError("Fail connect")
            self.channel.basic_publish(
                exchange=exchange, routing_key=routing_key, body=message
            )
        except Exception:
            raise
