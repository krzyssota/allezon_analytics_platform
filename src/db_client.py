import logging
from classes import UserTag, UserProfile, Action
from serde import serialize_tags, deserialize_tags
import aerospike
from aerospike_helpers.operations import operations as op_helpers

MAX_TAG_NUMBER = 200


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

    def add_tag(self, user_tag: UserTag) -> bool:
        for i in range(3):
            (user_profile, gen) = self.get_user_profile(user_tag.cookie)
            if not user_profile:  # new user
                user_profile = UserProfile.parse_obj({"cookie": user_tag.cookie, "buys": [], "views": []})

            if user_tag.action == Action.VIEW:
                if len(user_profile.views) == MAX_TAG_NUMBER:
                    user_profile.views.pop(0)
                user_profile.views.append(user_tag)
                user_profile.views = sorted(user_profile.views, key=lambda t: t.time)
            else:
                if len(user_profile.buys) == MAX_TAG_NUMBER:
                    user_profile.buys.pop(0)
                user_profile.buys.append(user_tag)
                user_profile.buys = sorted(user_profile.buys, key=lambda t: t.time)
            if self.put_user_profile(user_profile, gen):
                return True
            else:
                print(f"{user_tag.cookie} couldn't add tag for the {i}. time: {user_tag}")
                continue
        return False

    def get_user_profile(self, cookie: str) -> (UserProfile, int):
        try:
            key = (self.namespace, self.set, cookie)
            if not self.client.is_connected():
                self.client.connect()
            (key, meta, bins_json) = self.client.get(key)
            # decided to get rid of decompression for better performance
            ser_bs = bins_json["buys"]    # = snappy.decompress(bins_json["buys"]).decode("utf-8")
            ser_vs = bins_json[ "views"]  # = snappy.decompress(bins_json["views"]).decode("utf-8")
            bs = deserialize_tags(ser_bs)
            vs = deserialize_tags(ser_vs)
            res = UserProfile.parse_obj({"cookie": cookie, "buys": bs, "views": vs})
            return res, meta["gen"]
        except aerospike.exception.RecordNotFound:
            return None, 0  # new user
        except aerospike.exception.AerospikeError as e:
            print(f"Error {e} while reading user {cookie} profile")

    def put_user_profile(self, user_profile: UserProfile, gen: int) -> bool:
        try:
            key = (self.namespace, self.set, user_profile.cookie)
            ser_bs = serialize_tags(user_profile.buys)
            ser_vs = serialize_tags(user_profile.views)
            # decided to get rid of decompression for better performance
            comp_bs = ser_bs  # = snappy.compress(ser_bs)
            comp_vs = ser_vs  # = snappy.compress(ser_vs)
            write_policy = {"gen": aerospike.POLICY_GEN_EQ}
            ops = [
                op_helpers.write("cookie", user_profile.cookie),
                op_helpers.write("buys", comp_bs),
                op_helpers.write("views", comp_vs)
            ]
            if not self.client.is_connected():
                self.client.connect()
            self.client.operate(key, ops, policy=write_policy, meta={"gen": gen})
            return True
        except aerospike.exception.RecordGenerationError:  # record was modified in the meantime
            return False
        except aerospike.exception.AerospikeError as e:
            print(f"Error {e} while writing user {user_profile.cookie} profile")
            return False

    def close(self):
        self.client.close()
