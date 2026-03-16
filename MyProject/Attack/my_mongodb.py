import pymongo


client = pymongo.MongoClient("mongodb://root:Yb1226@mongodb:27017/mGoalsDB?authSource=admin")
db = client["GoalsDB"]
BankOfGoals = db["BankOfGoals"]

def up_data(data, _id):
    e = BankOfGoals.update_one({"_id": _id}, {"$set":data})
    return e


def is_search_query(entity_id):
    data = BankOfGoals.find_one({"entity_id": entity_id})
    try:
        data = data["entity_id"]
    except:
        return False
    return entity_id == data

def search_query(entity_id):
    return BankOfGoals.find_one({"entity_id": entity_id})

def search_query_id(entity_id):
    result = BankOfGoals.find_one(
        {"entity_id": entity_id},
        {"_id": 1}
    )
    if result:
        return result["_id"]
    return None