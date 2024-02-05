from psycopg2 import connect
from typica import BaseConnection, ConnectionMeta, ConnectionUriMeta


class PostgreRepository(BaseConnection):
    def __init__(self, metadata: ConnectionMeta | ConnectionUriMeta, **kwargs) -> None:
        super().__init__(metadata)
        try:
            self.conn = connect(
                **self._metadata.model_dump(
                    exclude={"uri", "type_connection", "clustersUri", "username"}
                ),
                user=self._metadata.username,
                **kwargs,
            )
            self.client = self.conn.cursor()
        except Exception:
            raise

    def close(self):
        self.client.close()
