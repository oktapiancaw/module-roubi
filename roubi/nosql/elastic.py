import re

from elasticsearch7 import Elasticsearch, helpers
from typica import BaseConnection, ConnectionMeta, ConnectionUriMeta


class ElasticRepository(BaseConnection):
    def __init__(self, metadata: ConnectionMeta | ConnectionUriMeta, **kwargs) -> None:
        super().__init__(metadata)

        
        if isinstance(self._metadata, ConnectionUriMeta) and self._metadata.uri:
            _client_host = self._metadata.uri
        else:
            if self._metadata.username:
                _client_host = f"http://{self._metadata.username}:{self._metadata.password}@{self._metadata.host}:{self._metadata.port}/"
            else:
                _client_host = f"http://{self._metadata.host}:{self._metadata.port}/"
                
        self.client: Elasticsearch = Elasticsearch(_client_host, **kwargs)
        
    
    def close(self):
        self.client.close()

    
    def get_manual_index_pattern(self, database: str = '*') -> list:
        try:
            if not self.client:
                raise ValueError("Fail connect")
            regex_formula = (
                r"(([-_]\d{13})|([-_]\d{4}([-_]\d{2}){1,2})|([-_]\d{4,8})|([-_]\d{1,3}))"
            )
            regex_formula2 = r"([-\w]+([-_]\d{13})|[-\w]+([-_]\d{4}([-_]\d{2}){1,2})|[-\w]+([-_]\d{4,8})|[-\w]+([-_]\d{1,3}))"
            indices = {}
            raw_indices = list(self.client.indices.get_alias(database if database else self._metadata.database).keys())
            raw_indices.sort()
            total_index = {}
            for index in raw_indices:
                clean_index = (
                    f'{re.sub(regex_formula, "", index)}-*'
                    if re.fullmatch(regex_formula2, index)
                    else index
                )
                if "." in index or "{" in index:
                    continue
                else:
                    total_index[clean_index] = (
                        1
                        if clean_index not in total_index
                        else total_index[clean_index] + 1
                    )
                    indices[clean_index] = {
                        "indexName": index,
                        "cleanIndexName": clean_index,
                        "totalIndex": total_index[clean_index],
                        "isIndexPattern": True
                        if re.fullmatch(regex_formula2, index)
                        else False,
                    }
            return list(indices.values())
        except Exception:
            raise
    