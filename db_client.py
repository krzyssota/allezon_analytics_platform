import logging
from typing import Optional
import time
import aerospike
from aerospike_helpers.operations import operations as op_helpers
from snappy import snappy

from classes import UserTag, UserProfile, Action
from serde import deserialize_user_profile, serialize_user_profile, serialize_tags, deserialize_tags

MAX_TAG_NUMBER = 200


class MyAerospikeClient:
    namespace = "mimuw"
    set = "tags"
    config = {
        'hosts': [
            ('10.112.109.103', 3000),
            ('10.112.109.104', 3000),
            ('10.112.109.105', 3000),
            ('10.112.109.106', 3000)
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
            ser_bs = snappy.decompress(record["buys"]).decode("utf-8")
            ser_vs = snappy.decompress(record["views"]).decode("utf-8")
            bs = deserialize_tags(ser_bs)
            vs = deserialize_tags(ser_vs)
            up = UserProfile.parse_obj({"cookie": record["cookie"], "buys": bs, "views": vs})
            print(f"print scan result {up}")

        scan = self.client.scan(self.namespace)
        scan.foreach(print_result)

    def delete_key(self, key: str) -> bool:
        try:
            self.client.remove((self.namespace, self.set, key))
            return True
        except aerospike.exception.RecordError:
            self.logger.error("key to delete not found: %s", key)
            return False

    def add_tag(self, user_tag: UserTag) -> bool:
            (user_profile, gen) = self.get_user_profile(user_tag.cookie, -1)
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

            return self.put_user_profile(user_profile, gen, -1)
    def old_add_tag(self, user_tag: UserTag):
        for i in range(3):
            (user_profile, gen) = self.get_user_profile(user_tag.cookie, i)
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

            if self.put_user_profile(user_profile, gen, i):
                return
            else:
                time.sleep(0.01)

    def get_user_profile(self, cookie: str, i: int):
        try:
            key = (self.namespace, self.set, cookie)
            (key, meta, bins_json) = self.client.get(key)
            ser_bs = snappy.decompress(bins_json["buys"]).decode("utf-8")
            ser_vs = snappy.decompress(bins_json["views"]).decode("utf-8")
            bs = deserialize_tags(ser_bs)
            vs = deserialize_tags(ser_vs)
            res = UserProfile.parse_obj({"cookie": cookie, "buys": bs, "views": vs})
            t2 = time.time()
            return res, meta["gen"]
        except aerospike.exception.RecordNotFound:
            return None, 1  # new user
        except aerospike.exception.AerospikeError as e:
            print(f"error reading user_profile(%s) %s", cookie, e)

    def put_user_profile(self, user_profile: UserProfile, gen: int, i: int) -> bool:
        try:
            key = (self.namespace, self.set, user_profile.cookie)
            ser_bs = serialize_tags(user_profile.buys)
            ser_vs = serialize_tags(user_profile.views)
            comp_bs = snappy.compress(ser_bs)
            comp_vs = snappy.compress(ser_vs)
            write_policy = {"gen": aerospike.POLICY_GEN_EQ}
            self.client.put(key, {"cookie": user_profile.cookie, "buys": comp_bs, "views": comp_vs}, policy=write_policy, meta={"gen": gen})
            return True
        except aerospike.exception.RecordGenerationError:
            #print(f"{i + 1}. generation error while trying to write user profile for: {user_profile.cookie}")
            return False
        except aerospike.exception.AerospikeError as e:
            print(f"error writing user_profile(%s) %s", user_profile.cookie, e)
            return False

    def close(self):
        self.client.close()
