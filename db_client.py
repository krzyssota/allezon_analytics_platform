import logging
from typing import Optional

import aerospike
from snappy import snappy

from classes import UserTag, UserProfile, Action
from serde import deserialize_user_profile, serialize_user_profile, serialize_tags, deserialize_tags

MAX_TAG_NUMBER = 200


class MyAerospikeClient:
    namespace = "mimuw"
    set = "tags"
    config = {
        'hosts': [
            ('10.112.109.101', 3000),
            ('10.112.109.102', 3000)
        ],
        'policies': {
            'timeout': 5000  # milliseconds
        }
    }
    logger = logging.getLogger()
    client = aerospike.client(config)

    def __init__(self):
        self.logger.setLevel(logging.DEBUG)
        self.client.connect()

    def delete_all_records(self):
        self.client.truncate(self.namespace, self.set, 0)
    def log_all_records(self):
        def print_result(record_tuple):
            key, metadata, record = record_tuple
            compressed = record["compressed"]
            decompressed = snappy.decompress(compressed).decode("utf-8")
            user_profile = deserialize_user_profile(decompressed)
            print(f"print scan result {user_profile}")

        scan = self.client.scan(self.namespace)
        scan.foreach(print_result)

    def delete_key(self, key: str) -> bool:
        try:

            self.client.remove((self.namespace, self.set, key))
            return True
        except aerospike.exception.RecordError:
            self.logger.error("key to delete not found: %s", key)
            return False

    def add_tag(self, user_tag: UserTag):

        user_profile = self.get_user_profile(user_tag.cookie)
        if not user_profile:
            user_profile = UserProfile.parse_obj({"cookie": user_tag.cookie, "buys": [], "views": []})

        if user_tag.action == Action.VIEW:
            if len(user_profile.views) == MAX_TAG_NUMBER:
                user_profile.views.pop(0)
            user_profile.views.append(user_tag)
        else:
            if len(user_profile.buys) == MAX_TAG_NUMBER:
                user_profile.buys.pop(0)
            user_profile.buys.append(user_tag)

        self.put_user_profile(user_profile)

    def get_user_profile(self, cookie: str) -> Optional[UserProfile]:
        try:
            key = (self.namespace, self.set, cookie)
            (key, meta, bins_json) = self.client.get(key)
            ser_bs = snappy.decompress(bins_json["buys"]).decode("utf-8")
            ser_vs = snappy.decompress(bins_json["views"]).decode("utf-8")
            bs = deserialize_tags(ser_bs)
            vs = deserialize_tags(ser_vs)
            return UserProfile.parse_obj({"cookie": cookie, "buys": bs, "views": vs})
        except aerospike.exception.RecordNotFound:
            return None  # new user
        except aerospike.exception.AerospikeError as e:
            print(f"error reading user_profile(%s) %s", cookie, e)

    def put_user_profile(self, user_profile: UserProfile):
        try:
            key = (self.namespace, self.set, user_profile.cookie)
            ser_bs = serialize_tags(user_profile.buys)
            ser_vs = serialize_tags(user_profile.views)
            comp_bs = snappy.compress(ser_bs)
            comp_vs = snappy.compress(ser_vs)
            self.client.put(key, {"cookie": user_profile.cookie, "buys": comp_bs, "views": comp_vs})
        except aerospike.exception.AerospikeError as e:
            print(f"error writing user_profile(%s) %s", user_profile.cookie, e)

    def close(self):
        self.client.close()