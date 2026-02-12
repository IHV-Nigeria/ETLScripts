from pymongo import MongoClient


def get_db_connection(db_name="ihvn"):
    """Established connection to MongoDB and returns the database object."""
    client = MongoClient("mongodb://localhost:27017/",datetime_conversion="DATETIME_AUTO")
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


# Get all containers where messageHeader.facilityDatimCode is in the provided list of datim codes   
def get_containers_by_datim_list(db, datim_codes, db_name="ihvn"):
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

def get_container_by_datim_list_size(db, datim_codes, db_name="cdr"):
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

def get_all_facilities(db,db_name="ihvn"):
    if(db is None):
        db = get_db_connection(db_name)
    # We sort by State and FacilityName to keep your logs and 
    # output folders organized.
    
    facilities_cursor = db.facilities.find().sort([("State", 1), ("FacilityName", 1)])
    return list(facilities_cursor)

