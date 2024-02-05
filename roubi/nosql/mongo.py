from pymongo import MongoClient
from pymongo.collection import Collection

from typica import (
    BaseConnection,
    ConnectionMeta,
    ConnectionUriMeta,
    Pagination,
    MultiFilterSchemas,
    DataValidResponse,
    DataStatus,
)


class MongoRepository(BaseConnection):
    def __init__(self, metadata: ConnectionMeta | ConnectionUriMeta, **kwargs) -> None:
        super().__init__(metadata)

        if isinstance(self._metadata, ConnectionUriMeta) and self._metadata.uri:
            _client_host = self._metadata.uri
        else:
            if self._metadata.username:
                _client_host = f"mongodb://{self._metadata.username}:{self._metadata.password}@{self._metadata.host}:{self._metadata.port}/"
            else:
                _client_host = f"mongodb://{self._metadata.host}:{self._metadata.port}/"

        try:
            self.client: MongoClient = MongoClient(_client_host, **kwargs)
        except:
            raise

        self.db = self.client[self._metadata.database]

    def close(self):
        self.client.close()

    def count(self, collection: str, query: dict = {}, database: str = None) -> any:
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col = self.client[database if database else self._metadata.database][
                collection
            ]
            result: int | None = col.count_documents(query)
            return result

        except Exception:
            raise

    def get_collection(self, collection: str, database: str = None) -> any:
        try:
            if not self.client:
                raise ValueError("Fail connect")

            conn = self.client[database if database else self._metadata.database]
            result: any | None = conn.get_collection(collection)
            return result

        except Exception:
            raise

    def list_collection(self, database: str = None) -> any:
        try:
            if not self.client:
                raise ValueError("Fail connect")

            conn = self.client[database if database else self._metadata.database]
            result: list | None = conn.list_collections(
                filter={
                    "$and": [
                        {"name": {"$nin": ["system.views", "*"]}},
                        {"name": {"$not": {"$regex": "v_*", "$options": "i"}}},
                    ]
                }
            )
            return result

        except Exception:
            raise

    def find_one(
        self,
        collection: str,
        query: dict = {},
        filter: dict = None,
        database: str = None,
    ) -> any:
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result: any | None = col.find_one(query, filter)
            return result

        except Exception:
            raise

    def find(
        self,
        collection: str,
        query: dict = {},
        filter: dict = None,
        database: str = None,
    ) -> any:
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result: list | None = list(col.find(query, filter))
            return result

        except Exception:
            raise

    def find_paginate(
        self,
        collection: str,
        parameter: MultiFilterSchemas,
        additional_query: dict = {},
        filter: dict = None,
        include_archive: bool = False,
        database: str = None,
    ) -> list[list, Pagination]:
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            query = {"$and": []}
            if parameter.filters:
                for filter_param in parameter.filters:
                    if filter_param.field and filter_param.value:
                        if isinstance(filter_param.value, str):
                            query["$and"].append(
                                {
                                    filter_param.field: {
                                        "$regex": filter_param.value,
                                        "$options": "i",
                                    }
                                }
                            )
                        else:
                            query["$and"].append(
                                {filter_param.field: filter_param.value}
                            )
            if additional_query:
                query["$and"].append(additional_query)
            if not include_archive:
                query["$and"].append({"status": {"$ne": DataStatus.archive.value}})

            if parameter.timeframe and parameter.timeframe.gte:
                query["$and"].append(
                    {
                        parameter.timeframe.field: {
                            "$gte": parameter.timeframe.gte,
                            "$lte": parameter.timeframe.lte,
                        }
                    }
                )
            if not query["$and"]:
                del query["$and"]
            len_data = col.count_documents(query)
            if len_data == 0:
                return [], Pagination(
                    size=parameter.size, totalPages=0, totalItems=len_data
                )

            result = col.find(query, filter)
            if parameter.orderBy:
                result = result.sort(
                    parameter.orderBy, 1 if parameter.order == "ASC" else -1
                )

            total_pages = (
                int(len_data / parameter.size) if len_data > parameter.size else 1
            )
            if parameter.page:
                result = result.skip((parameter.page - 1) * parameter.size).limit(
                    parameter.size * parameter.page
                )
            else:
                result = result.limit(parameter.size)
            return [
                list(result) if result else None,
                Pagination(
                    size=parameter.size, totalPages=total_pages, totalItems=len_data
                ),
            ]

        except Exception:
            raise

    def insert_one(
        self,
        collection: str,
        data: dict,
        database: str = None,
        unique_keys: list = [],
        **kwargs,
    ) -> DataValidResponse:
        try:
            if not self.client:
                raise ValueError("Fail connect")
            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            if unique_keys:
                query_unique = {
                    "$or": [
                        {key: data[key]} for key in unique_keys if key in data.keys()
                    ]
                }
                if col.find_one(query_unique):
                    return DataValidResponse(
                        status=False, detail="unique validation", data=unique_keys
                    )

            result = col.insert_one(data, **kwargs)
            return DataValidResponse(status=True, data=result)

        except Exception:
            raise

    def insert_many(self, collection: str, data: list, database: str = None, **kwargs):
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result = col.insert_many(data, **kwargs)
            return result

        except Exception:
            raise

    def update_one(
        self, collection: str, query: dict, data: dict, database: str = None, **kwargs
    ):
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result = col.update_one(query, data, **kwargs)
            return result

        except Exception:
            raise

    def update_many(
        self, collection: str, query: dict, data: dict, database: str = None, **kwargs
    ):
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result = col.update_many(query, data, **kwargs)
            return result

        except Exception:
            raise

    def archive_one(self, collection: str, query: dict, database: str = None, **kwargs):
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result = col.update_one(
                query, {"$set": {"status": DataStatus.archive}}, **kwargs
            )
            return result

        except Exception:
            raise

    def archive_many(
        self, collection: str, query: dict, database: str = None, **kwargs
    ):
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result = col.update_many(
                query, {"$set": {"status": DataStatus.archive}}, **kwargs
            )
            return result

        except Exception:
            raise

    def delete_one(self, collection: str, query: dict, database: str = None, **kwargs):
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result = col.delete_one(query, **kwargs)
            return result

        except Exception:
            raise

    def delete_many(self, collection: str, query: dict, database: str = None, **kwargs):
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result = col.delete_many(query, **kwargs)
            return result

        except Exception:
            raise

    def aggregate(
        self, collection: str, agg_query: list, database: str = None, **kwargs
    ):
        try:
            if not self.client:
                raise ValueError("Fail connect")

            col: Collection = self.client[
                database if database else self._metadata.database
            ][collection]
            result = list(col.aggregate(agg_query, **kwargs))

            return result

        except Exception:
            raise
