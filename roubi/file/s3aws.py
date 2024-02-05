import boto3
from botocore.exceptions import ClientError
from pydantic import TypeAdapter
from typica import BaseConnection, ConnectionMeta, ConnectionUriMeta, BaseModel


from roubi.models.s3aws import listObjectRes


class RedisRepository(BaseConnection):
    def __init__(self, metadata: ConnectionMeta | ConnectionUriMeta, **kwargs) -> None:
        super().__init__(metadata)
        try:
            self.client = boto3.client(
                "s3",
                endpoint_url=f"http://{self._metadata.host}:{self._metadata.port}",
                aws_access_key_id=self._metadata.username,
                aws_secret_access_key=self._metadata.password,
            )

            self.bucket = self._metadata.database

        except Exception:
            raise

    def check_accessible(self, key: str, bucket: str = None) -> bool:
        try:
            if self.client == False:
                raise ValueError("Fail connect")

            self.client.get_object(Bucket=bucket if bucket else self.bucket, Key=key)
            return True
        except ClientError as e:
            return False

    def list_file(
        self,
        excludeFormat: list[str] = ["cryptomancer", "trashinfo"],
        bucket: str = None,
    ) -> bool:
        try:
            if self.client == False:
                raise ValueError("Fail connect")

            raw = TypeAdapter(listObjectRes).validate_python(
                self.client.list_objects(Bucket=bucket if bucket else self.bucket)
            )
            return raw.filter_file(excludeFormat=excludeFormat)
        except ClientError as e:
            raise

    def list_folder(
        self,
        excludeFormat: list[str] = ["cryptomancer", "trashinfo"],
        bucket: str = None,
    ) -> bool:
        try:
            if self.client == False:
                raise ValueError("Fail connect")

            raw = TypeAdapter(listObjectRes).validate_python(
                self.client.list_objects(Bucket=bucket if bucket else self.bucket)
            )
            return raw.filter_folder(excludeFormat=excludeFormat)
        except ClientError as e:
            raise
