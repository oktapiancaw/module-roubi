from datetime import datetime

from pydantic import field_serializer, model_validator
from typica import BaseModel, Optional, Field


class OwnerObject(BaseModel):
    displayName: str = Field(..., alias="DisplayName")
    id: str = Field(..., alias="ID")


class ObjectMeta(BaseModel):
    isFolder: Optional[bool] = Field(False, description="This will be generated")
    fileFormat: Optional[str] = Field("", description="This will be generated")
    name: Optional[str] = Field("", description="This will be generated")
    key: str = Field(..., alias="Key")
    lastModified: datetime = Field(..., alias="LastModified")
    size: int = Field(..., alias="Size")
    storageClass: str = Field(..., alias="StorageClass")
    owner: Optional[OwnerObject] = Field(None, alias="Owner")

    @field_serializer("lastModified")
    def datetime(self, lastModified: datetime):
        return int(lastModified.timestamp() * 1000)

    @model_validator(mode="after")
    def validate_object(self):
        if self.size == 0 and self.key.endswith("/"):
            self.isFolder = True
        if self.key and "." in self.key:
            self.fileFormat = self.key.split(".")[-1]
        if self.key:
            self.name = self.key.split("/")[-1]
        return self

    @property
    def is_dataframe(self):
        return self.fileFormat in ["csv", "xls", "xlsx"]


class listObjectRes(BaseModel):
    # marker: str = Field()
    contents: Optional[list[ObjectMeta]] = Field([], alias="Contents")
    name: str = Field(..., alias="Name")
    prefix: str = Field(..., alias="Prefix")
    delimiter: Optional[str] = Field(None, alias="Delimiter")

    def filter_file(
        self, excludeFormat: list[str] = ["cryptomancer", "trashinfo"]
    ) -> list[ObjectMeta]:
        return [
            content
            for content in self.contents
            if not content.isFolder and content.fileFormat not in excludeFormat
        ]

    def filter_folder(self) -> list[ObjectMeta]:
        return [content for content in self.contents if content.isFolder]
