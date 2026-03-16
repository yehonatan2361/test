import pymongo




client = pymongo.MongoClient("mongodb://root:Yb1226@mongodb:27017/mGoalsDB?authSource=admin")
db = client["GoalsDB"]
BankOfGoals = db["BankOfGoals"]

def add_data(data):
    return BankOfGoals.insert_one(data)


def is_search_query(entity_id):
    data = BankOfGoals.find_one({"entity_id": entity_id})
    try:
        data = data["entity_id"]
    except:
        return False
    return entity_id == data

def search_query(entity_id):
    return BankOfGoals.find_one({"entity_id": entity_id})

def kk(entity_id):
    return BankOfGoals.find({"entity_id": entity_id}, {"_id": 0, "lat_reported": 1, "lon_reported": 1})


