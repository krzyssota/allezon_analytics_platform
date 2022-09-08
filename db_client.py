import json
import logging
import aerospike
import snappy

from classes import UserTag, UserProfile, Action

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

    def log_all_records(self):
        def print_result(record_tuple):
            key, metadata, record = record_tuple
            print("print scan result %s %s %s", key, metadata, record)
            self.logger.info("log scan result: %s %s %s", key, metadata, record)

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
        user_tag_json = {
            'time': user_tag.time,
            'cookie': user_tag.cookie,
            'country': user_tag.country,
            'device': user_tag.device,
            'action': user_tag.action,
            'origin': user_tag.origin,
            'product_info': {
                'product_id': user_tag.product_info["product_id"],
                'brand_id': user_tag.product_info["brand_id"],
                'category_id': user_tag.product_info["category_id"],
                'price': user_tag.product_info["price"]
            }
        }
        user_profile = self.get_user_profile(user_tag.cookie)

        if user_tag.action == Action.VIEW:
            if len(user_profile.views) == MAX_TAG_NUMBER:
                user_profile.views.pop(0)
            user_profile.views.append(user_tag)
        else:
            if len(user_profile.buys) == MAX_TAG_NUMBER:
                user_profile.buys.pop(0)
            user_profile.buys.append(user_tag)

        self.put_user_profile(user_profile)

        # encoded: str = json.dumps(user_tag_json)
        # compressed = snappy.compress(encoded)

        # self.client.write(compressed)

        # TODO wymyslic jak przechowywać (UserResult = (cookie:str, views, buys) ?)
        # dokończyć add_tag =
        # napisać get_user_profile

    def get_user_profile(self, cookie: str) -> UserProfile:
        try:
            key = (self.namespace, self.set, cookie)
            (key, meta, bins_json) = self.client.get(key)
            compressed = bins_json["compressed_profile"]
            uncompressed = snappy.uncompress(compressed)
            return UserProfile(uncompressed)
        except aerospike.exception.AerospikeError as e:
            print(f"error reading user_profile(%s) %s", cookie, e)

    def put_user_profile(self, user_profile: UserProfile):
        try:
            key = (self.namespace, self.set, user_profile.cookie)
            user_profile_json = json.dumps(user_profile.__dict__)
            compressed = snappy.compress(user_profile_json)
            self.client.put(key, {"compressed_profile": compressed})
        except aerospike.exception.AerospikeError as e:
            print(f"error writing user_profile(%s) %s", user_profile.cookie, e)
