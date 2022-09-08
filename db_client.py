import logging
import aerospike

config = {
    'hosts': [
        ('10.112.109.101', 3000),
        ('10.112.109.102', 3000)
    ],
    'policies': {
        'timeout': 1000  # milliseconds
    }
}
NAMESPACE = "mimuw"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
client = aerospike.client(config)
client.connect()


def print_result(record_tuple):
    key, metadata, record = record_tuple
    print(key, metadata, record)
    logger.info("scan result: %s %s %s", key, metadata, record)

scan = client.scan(NAMESPACE)
scan.foreach(print_result)
