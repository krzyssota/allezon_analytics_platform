import logging
import aerospike

class MyAerospikeClient:
    namespace = "mimuw"
    config = {
        'hosts': [
            ('10.112.109.101', 3000),
            ('10.112.109.102', 3000)
        ],
        'policies': {
            'timeout': 1000  # milliseconds
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
            print(key, metadata, record)
            self.logger.info("scan result: %s %s %s", key, metadata, record)

        scan = self.client.scan(self.namespace)
        scan.foreach(print_result)


