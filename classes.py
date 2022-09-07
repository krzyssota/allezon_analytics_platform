from dataclasses import dataclass
from enum import Enum
from datetime import datetime
from typing import List

from typing_extensions import TypedDict

import pydantic
from pydantic import BaseModel

class Config:
    arbitrary_types_allowed = True

class Device(Enum):
    PC = "PC"
    MOBILE = "MOBILE"
    TV = "TV"


class Action(Enum):
    VIEW = "VIEW"
    BUY = "BUY"


class Aggregate(Enum):
    COUNT = "COUNT"
    SUM_PRICE = "SUM_PRICE"

class ProductInfo(TypedDict):
    product_id: str
    brand_id: str
    category_id: str
    price: int

class UserTag(BaseModel):
    time: datetime
    cookie: str
    country: str
    device: Device
    action: Action
    origin: str
    product_info: ProductInfo

class UserProfileResult(BaseModel):
    cookie: str
    buys: List[UserTag]
    views: List[UserTag]
