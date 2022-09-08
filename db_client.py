import logging
import aerospike

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

