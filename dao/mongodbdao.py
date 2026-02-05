from pymongo import MongoClient


def get_db_connection(db_name="ihvn"):
    """Established connection to MongoDB and returns the database object."""
    client = MongoClient("mongodb://localhost:27017/")
    return client[db_name]

def get_art_container_size(db,db_name="ihvn"):
    """Returns the count of ART containers in the database."""
    if(db is None):
        db = get_db_connection(db)
    query = {
    "messageData.patientIdentifiers": {
        "$elemMatch": {
            "identifierType": 4,
            "voided": 0
        }
    }
    }
    art_containers_count = db.container.count_documents(query)
    return art_containers_count

def get_art_containers(db,db_name="ihvn"):
    if(db is None):
        db = get_db_connection(db_name)
    query = {
    "messageData.patientIdentifiers": {
        "$elemMatch": {
            "identifierType": 4,
            "voided": 0
        }
    }
    }
    art_containers_cusor = db.container.find(query)
    return art_containers_cusor