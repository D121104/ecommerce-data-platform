from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
)


class ApiModel(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
        str_strip_whitespace=True,
    )


class Category(ApiModel):
    id: int = Field(gt=0)
    name: str = Field(min_length=1)
    slug: str | None = None

    image_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("image", "image_url"),
    )

    creation_at: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("creationAt", "creation_at"),
    )

    updated_at: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("updatedAt", "updated_at"),
    )


class Product(ApiModel):
    id: int = Field(gt=0)
    title: str = Field(min_length=1)
    slug: str | None = None

    price: Decimal = Field(ge=0)
    description: str | None = None

    category: Category | None = None
    images: list[str] = Field(default_factory=list)

    creation_at: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("creationAt", "creation_at"),
    )

    updated_at: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("updatedAt", "updated_at"),
    )


class User(ApiModel):
    id: int = Field(gt=0)
    email: EmailStr
    name: str = Field(min_length=1)

    role: Literal["customer", "admin"] | None = None

    avatar_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("avatar", "avatar_url"),
    )

    creation_at: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("creationAt", "creation_at"),
    )

    updated_at: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("updatedAt", "updated_at"),
    )
