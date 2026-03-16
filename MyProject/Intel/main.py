import consumer
import time
import my_mongodb
import haversine
import producer
import json
import logging
from logger import log_event

logging.basicConfig(
    level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)


def check_data(data):
    logger.info("check mongo")
    if my_mongodb.is_search_query(data["entity_id"]):
        data_ = my_mongodb.search_query(data["entity_id"])
        try:
            result = data_["result"]
            return False
        except:
            return True
    return True


def mongo_(data):
    logger.info("mongo")
    try:
        timestamp = data["timestamp"]
    except:
        timestamp = 0
    try:
        signal_id = data["signal_id"]
    except:
        signal_id = 0
    try:
        entity_id = data["entity_id"]
    except:
        entity_id = 0
    try:
        reported_lat = data["reported_lat"]
    except:
        reported_lat = 0
    try:
        reported_lon = data["reported_lon"]
    except:
        reported_lon = 0
    try:
        signal_type = data["signal_type"]
    except:
        signal_type = 0
    try:
        priority_level = data["priority_level"]
    except:
        priority_level = 0
    if not my_mongodb.is_search_query(data["entity_id"]):
        logger.info("not in mongo")
        data = {
            "timestamp": timestamp,
            "signal_id": signal_id,
            "entity_id": entity_id,
            "reported_lat": reported_lat,
            "reported_lon": reported_lon,
            "signal_type": signal_type,
            "priority_level": 99,
            "distance": 0
        }
    else:
        logger.info("data is mongo")
        d = my_mongodb.kk(data["entity_id"])
        try:
            distance = haversine.haversine_km(reported_lat, reported_lon, d["reported_lat"], d["reported_lon"])
        except:
            distance = "unknown"
        data = {
            "timestamp": timestamp,
            "signal_id": signal_id,
            "entity_id": entity_id,
            "reported_lat": reported_lat,
            "reported_lon": reported_lon,
            "signal_type": signal_type,
            "priority_level": priority_level,
            "distance": distance
        }

    if check_data(data):
        e = my_mongodb.add_data(data)
        logger.info(e)




def check(data):
    try:
        data = json.loads(data)
        if "entity_id" not in data or "signal_id" not in data:
            logger.info("error: Required fields are missing")
            producer.sending_data({"data": data, "err": "Required fields are missing"})
            return False
        return True
    except json.JSONDecodeError:
        logger.info("error: broken JSON")
        producer.sending_data({"data": str(data), "err": "broken JSON"})
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


