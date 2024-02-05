from clickhouse_connect import get_client
from typica import BaseConnection, ConnectionMeta, ConnectionUriMeta


class CHRepository(BaseConnection):
    def __init__(self, metadata: ConnectionMeta | ConnectionUriMeta, **kwargs) -> None:
        super().__init__(metadata)
        try:
            self.client = get_client(
                **self._metadata.model_dump(
                    exclude={"uri", "type_connection", "clustersUri"}, **kwargs
                )
            )
        except Exception:
            raise

    def close(self):
        self.client.close()
