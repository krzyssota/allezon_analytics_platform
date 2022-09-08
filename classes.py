import json
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
from typing import List, Any
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

    def serialize_tag(self) -> str:
        res1 = f"{self.time}|{self.cookie}|{self.country}|{self.device.name}|{self.action.name}|{self.origin}|"
        res2 = self.product_info["product_id"] + "|" + self.product_info["brand_id"] + "|" + self.product_info[
            "category_id"] + "|" + str(self.product_info["price"])
        return res1 + res2


def serialize_tags(tags: List[UserTag]) -> str:
    return "^".join(list(map(lambda t: t.serialize_tag(), tags)))

def deserialize_tag(ser_tag: str) -> UserTag:
    ss = ser_tag.split("|")
    #print("deserialize_tag ", ss, ser_tag, len(ss))
    if ser_tag == "":
        return None
    ut_obj = {"time": ss[0], "cookie": ss[1], "country": ss[2], "device": ss[3], "action": ss[4],
              "origin": ss[5], "product_info": {"product_id": ss[6], "brand_id": ss[7], "category_id": ss[8], "price": int(ss[9])}}
    return UserTag.parse_obj(ut_obj)

def deserialize_tags(ser_tags: str) -> List[UserTag]:
    sts = ser_tags.split("^")
    if sts == [""]:
        return []
    return list(map(lambda st: deserialize_tag(st), sts))



class UserProfile(BaseModel):
    cookie: str
    buys: List[UserTag]
    views: List[UserTag]

    def serialize_user_profile(self) -> str:
        return self.cookie + "@" + serialize_tags(self.buys) + "@" + serialize_tags(self.views)

def deserialize_user_profile(ser_user_profile: str) -> UserProfile:
    ss = ser_user_profile.split('@')
    #print("deserialize_user_profile ", ss, len(ss), ser_user_profile)

    bs = deserialize_tags(ss[1])
    vs = deserialize_tags(ss[2])
    up_obj = {"cookie": ss[0], "buys": bs, "views": vs}
    return UserProfile.parse_obj(up_obj)