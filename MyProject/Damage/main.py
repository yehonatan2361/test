import consumer
import time
import my_mongodb
import json
import logging
from logger import log_event


logging.basicConfig(
    level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)



def mongo_(data):
    logger.info("mongo")
    logger.info(data)
    try:
        timestamp = data["timestamp"]
    except:
        timestamp = 0
    try:
        result = data["result"]
    except:
        result = 0
    entity_id = data["entity_id"]
    if my_mongodb.is_search_query(data["entity_id"]):
        logger.info("in mongo")
        data = {
            "timestamp_damage": timestamp,
            "result": result
        }

    _id = my_mongodb.search_query_id(entity_id)
    e = my_mongodb.up_data(data, _id)
    logger.info(8888888888)
    logger.info(e)



def check(data):
    try:
        data = json.loads(data)
        if "entity_id" not in data or "attack_id" not in data:
            logger.info("error: Required fields are missing")
            return False
        return True
    except json.JSONDecodeError:
        logger.info("error: broken JSON")
        return False



def main():
    try:
        while True:
            data = consumer.data_reader()
            if data == -1:
                time.sleep(1)
                continue
            if data == -2:
                consumer.close_consumer()
                continue
            if check(data):
                data = json.loads(data)
                mongo_(data)

    except Exception as e:
        logger.info(e)


if __name__ == "__main__":
    main()
