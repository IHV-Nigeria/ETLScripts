from pymongo import MongoClient
from . import config

def get_db_connection(db_name=config.MONGO_DATABASE_NAME):
    """Established connection to MongoDB and returns the database object."""
    client = MongoClient(f"mongodb://{config.MONGO_HOST}:{config.MONGO_PORT}/",datetime_conversion="DATETIME_AUTO")
    return client[db_name]

def get_art_container_size(db,db_name=config.MONGO_DATABASE_NAME):
    """Returns the count of ART containers in the database."""
    if(db is None):
        db = get_db_connection(config.MONGO_DATABASE_NAME)
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

def get_art_containers(db,db_name=config.MONGO_DATABASE_NAME):
    if(db is None):
        db = get_db_connection(config.MONGO_DATABASE_NAME)
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


# Get all containers where messageHeader.facilityDatimCode is in the provided list of datim codes   
def get_containers_by_datim_list(db, datim_codes, db_name=config.MONGO_DATABASE_NAME):
    """
    Retrieves all active ART containers belonging to a list of DATIM codes.
    """
    if db is None:
        db = get_db_connection(db_name)
    
    query = {
        # 1. Filter for active ART patients
            "messageData.patientIdentifiers": {
            "$elemMatch": {
                "identifierType": 4,
                "voided": 0
            }
        },
        # 2. Filter for specific facilities using the list of DATIM codes
        "messageHeader.facilityDatimCode": {
            "$in": datim_codes
        }
    }
    
    return db.container.find(query)

def get_container_by_datim_list_size(db, datim_codes, db_name=config.MONGO_DATABASE_NAME):
    """
    Retrieves the count of active ART containers belonging to a list of DATIM codes.
    """
    if db is None:
        db = get_db_connection(db_name)
    
    query = {
        # 1. Filter for active ART patients
        "messageData.patientIdentifiers": {
            "$elemMatch": {
                "identifierType": 4,
                "voided": 0
            }
        },
        # 2. Filter for specific facilities using the list of DATIM codes
        "messageHeader.facilityDatimCode": {
            "$in": datim_codes
        }
    }
    
    return db.container.count_documents(query)

def get_all_facilities(db,db_name=config.MONGO_DATABASE_NAME):
    if(db is None):
        db = get_db_connection(db_name)
    # We sort by State and FacilityName to keep your logs and 
    # output folders organized.
    
    facilities_cursor = db.facilities.find().sort([("State", 1), ("FacilityName", 1)])
    return list(facilities_cursor)

