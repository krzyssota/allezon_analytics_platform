import logging
from typing import Optional
import time
from snappy import snappy
from classes import UserTag, UserProfile, Action
from serde import serialize_tags, deserialize_tags
import aerospike
from aerospike_helpers.operations import operations as op_helpers

MAX_TAG_NUMBER = 200


def as_json(up):  # BEWARE IT CHNAGES UP TODO delete after debug
    def objectify(tag):
        tag.time = str(tag.time)
        tag.action = tag.action.name
        tag.device = tag.device.name
        return vars(tag)

    up.views = list(map(lambda v: objectify(v), up.views))
    up.buys = list(map(lambda b: objectify(b), up.buys))
    return vars(up)
class MyAerospikeClient:
    namespace = "mimuw"
    set = "tags"
    config = {
        'hosts': [
            ('10.112.109.103', 3000),
            ('10.112.109.104', 3000),
            ('10.112.109.105', 3000),
            ('10.112.109.106', 3000),
            ('10.112.109.108', 3000)
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

    def delete_key(self, key: str) -> bool:
        try:
            self.client.remove((self.namespace, self.set, key))
            return True
        except aerospike.exception.RecordError:
            self.logger.error("key to delete not found: %s", key)
            return False
    def add_tag(self, user_tag: UserTag) -> bool:
        for i in range(10):
            (user_profile, gen) = self.get_user_profile(user_tag.cookie, i)
            if not user_profile:
                user_profile = UserProfile.parse_obj({"cookie": user_tag.cookie, "buys": [], "views": []})

            if user_tag.action == Action.VIEW:
                if len(user_profile.views) == MAX_TAG_NUMBER:
                    user_profile.views.pop(0)
                user_profile.views.append(user_tag)
                sorted_vs = sorted(user_profile.views, key=lambda t: t.time)
                if sorted_vs != user_profile.views:
                    #print(f"{user_tag.cookie} views not sorted {(user_profile)}")
                    user_profile.views = sorted_vs
            else:
                if len(user_profile.buys) == MAX_TAG_NUMBER:
                    user_profile.buys.pop(0)
                user_profile.buys.append(user_tag)
                sorted_bs = sorted(user_profile.buys, key=lambda t: t.time)
                if sorted_bs != user_profile.buys:
                    #print(f"{user_tag.cookie} buys not sorted {(user_profile)}")
                    user_profile.buys = sorted_bs
            if self.put_user_profile(user_profile, gen, i):
                return True
            else:
                print(f"{user_tag.cookie} couldn't add tag for the {i}. time: {user_tag}")
                continue
        #print(f"{user_tag.cookie} couldn't add tag {user_tag}")
        return False

    def get_user_profile(self, cookie: str, i: int) -> (UserProfile, int):
        try:
            key = (self.namespace, self.set, cookie)
            if not self.client.is_connected():
                self.client.connect()
            (key, meta, bins_json) = self.client.get(key)
            ser_bs = bins_json["buys"] # = snappy.decompress(bins_json["buys"]).decode("utf-8") # TODO decide what to do with compression
            ser_vs = bins_json["views"] # = snappy.decompress(bins_json["views"]).decode("utf-8") # TODO decide what to do with compression
            bs = deserialize_tags(ser_bs)
            vs = deserialize_tags(ser_vs)
            res = UserProfile.parse_obj({"cookie": cookie, "buys": bs, "views": vs})
            return res, meta["gen"]
        except aerospike.exception.RecordNotFound:
            return None, 0  # new user
        except aerospike.exception.AerospikeError as e:
            print(f"error reading user_profile(%s) %s", cookie, e)

    def put_user_profile(self, user_profile: UserProfile, gen: int, i: int) -> bool:
        try:
            key = (self.namespace, self.set, user_profile.cookie)
            ser_bs = serialize_tags(user_profile.buys)
            ser_vs = serialize_tags(user_profile.views)
            comp_bs = ser_bs # = snappy.compress(ser_bs)  # TODO decide what to do with compression
            comp_vs = ser_vs # = snappy.compress(ser_vs)  # TODO decide what to do with compression
            write_policy = {"gen": aerospike.POLICY_GEN_EQ}
            ops = [
                op_helpers.write("cookie", user_profile.cookie),
                op_helpers.write("buys", comp_bs),
                op_helpers.write("views", comp_vs)
            ]
            if not self.client.is_connected():
                self.client.connect()
            self.client.operate(key, ops, policy=write_policy, meta={"gen": gen})
            #self.client.put(key, {"cookie": user_profile.cookie, "buys": comp_bs, "views": comp_vs},
            #                policy=write_policy, meta={"gen": gen})
            return True
        except aerospike.exception.RecordGenerationError:
            # print(f"{i + 1}. generation error while trying to write user profile for: {user_profile.cookie}")
            return False
        except aerospike.exception.AerospikeError as e:
            print(f"error writing user_profile(%s) %s", user_profile.cookie, e)
            return False

    def close(self):
        self.client.close()
