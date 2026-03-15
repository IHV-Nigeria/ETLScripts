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

def get_patient_recapture(doc):
    message_data = doc.get("messageData", {})
    patient_recapture = message_data.get("patientBiometricVerifications", [])
    return patient_recapture

def get_biometric_recapture_date(doc):
    patient_recapture = get_patient_recapture(doc)
    if not patient_recapture:
        return None
    # Assuming the recapture date is stored in a field called 'recaptureDate' within the biometric data
    recapture_date = patient_recapture[0].get("dateCreated")
    return recapture_date

def get_biometric_recapture_count(doc):
    patient_recapture = get_patient_recapture(doc)
    if not patient_recapture:
        return 0
    # Assuming the recapture count is stored in a field called 'recaptureCount' within the biometric data
    recapture_count = patient_recapture[0].get("recaptureCount")
    return recapture_count

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
   
