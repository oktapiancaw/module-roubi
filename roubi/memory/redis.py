from redis import Redis
from typica import BaseConnection, ConnectionMeta, ConnectionUriMeta


class RedisRepository(BaseConnection):
    def __init__(self, metadata: ConnectionMeta | ConnectionUriMeta, **kwargs) -> None:
        super().__init__(metadata)

        try:
            if isinstance(self._metadata, ConnectionUriMeta) and self._metadata.uri:
                self.client = Redis.from_url(self._metadata.uri, **kwargs)
            else:
                self.client = Redis(
                    **self._metadata.model_dump(
                        exclude={"uri", "clustersUri", "type_connection", "database"}
                    ),
                    db=self._metadata.database,
                    **kwargs
                )
        except Exception:
            raise

    def close(self):
        self.client.close()

    def add_data(self, key: str, value: str, expired: int = None):
        try:
            if self.client == False:
                raise ValueError("Fail connect")

            self.client.set(name=key, value=value, ex=expired)
        except Exception:
            raise

    def get_data(self, key: str):
        try:
            if self.client == False:
                raise ValueError("Fail connect")

            result = self.client.get(name=key)
            return result
        except Exception:
            raise

    def get_by_pattern(self, pattern: str):
        try:
            if self.client == False:
                raise ValueError("Fail connect")

            result = self.client.scan_iter(pattern)
            return result
        except Exception:
            raise

    def remove_data(self, key: str):
        try:
            if self.client == False:
                raise ValueError("Fail connect")

            self.client.delete(key)
        except Exception:
            raise

    def remove_by_pattern(self, pattern: str):
        try:
            if self.client == False:
                raise ValueError("Fail connect")
            for key in self.client.scan_iter(pattern):
                self.client.delete(key)
        except Exception:
            raise
