
def get_patient_identifier_data(doc):
    message_data = doc.get("messageData", {})
    patient_identifiers = message_data.get("patientIdentifiers", [])
    return patient_identifiers

def get_patient_identifier_by_type(doc, identifier_type):
    patient_identifiers = get_patient_identifier_data(doc)
    for identifier in patient_identifiers:
        if identifier.get("identifierType") == identifier_type and not identifier.get("voided", 0):
            return identifier
    return None

