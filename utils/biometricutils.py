def has_biometric_captured(doc):
    message_data = doc.get("messageData", [])
    patient_biometrics = message_data.get("patientBiometrics", [])
    if patient_biometrics:
        return True
    return False
def get_patient_biometric_data(doc):
    message_data = doc.get("messageData", {})
    patient_biometrics = message_data.get("patientBiometrics", [])
    return patient_biometrics


def get_biometric_capture_date(doc):
    patient_biometrics = get_patient_biometric_data(doc)
    if not patient_biometrics:
        return None
    # Assuming the capture date is stored in a field called 'captureDate' within the biometric data
    capture_date = patient_biometrics[0].get("dateCreated")
    return capture_date

def is_biometric_capture_valid(doc):
    patient_biometrics = get_patient_biometric_data(doc)
    if not patient_biometrics:
        return False
    isvalid=True
    for biometric in patient_biometrics:
        template = biometric.get("template")
        # check if template string begins with "Rk1S" 
        if not template or not template.startswith("Rk1S"):
            isvalid=False
            break
    return isvalid   
   
