from datetime import datetime, date
from typing import Final
# Popular Variables
ART_START_DATE_CONCEPT_ID = 159599
CARE_ENTRY_POINT_CONCEPT_ID = 160540
KP_TYPE_CONCEPT_ID = 166369
DATE_TRANSFERED_IN_CONCEPT_ID = 160534
PRIOR_ART_CONCEPT_ID = 165242
WEIGHT_KG_CONCEPT_ID = 5089
ARV_WRAPPING_CONCEPT_ID = 162240
ARV_MEDICATION_DURATION_CONCEPT_ID = 159368
PILL_BALANCE_CONCEPT_ID = 166406
CURRENT_REGIMEN_LINE_CONCEPT_ID =  	165708
CHILD_2ND_LINE_REGIMEN_CONCEPT_ID = 164514
ADULT_2ND_LINE_REGIMEN_CONCEPT_ID = 164513
ADULT_3RD_LINE_REGIMEN_CONCEPT_ID = 165702
CHILD_3RD_LINE_REGIMEN_CONCEPT_ID = 165703
VIRAL_LOAD_CONCEPT_ID = 856
SAMPLE_COLLECTION_DATE_CONCEPT_ID = 159951
VIRAL_LOAD_REPORTED_DATE_CONCEPT_ID = 165414
VIRAL_LOAD_INDICATION_CONCEPT_ID = 164980
REASON_FOR_TERMINATION_CONCEPT_ID = 165470
PREGNANCY_STATUS_CONCEPT_ID = 165050
EDD_CONCEPT_ID = 5596
EAC_SESSION_TYPE_CONCEPT_ID = 166097
EAC_BARRIERS_TO_ADHERENCE_CONCEPT_ID = 165457
EAC_REGIMEN_PLAN_CONCEPT_ID = 165771
EAC_FOLLOWUP_DATE_CONCEPT_ID = 165036
EAC_ADHERENCE_COMMENTS_CONCEPT_ID = 165606
DSD_MODEL_CONCEPT_ID = 166148
FACILITY_DSD_MODEL_CONCEPT_ID = 166276
DDD_DSD_MODEL_CONCEPT_ID = 166363
MMD_CONCEPT_ID = 166278
NEXT_APPOINTMENT_DATE_CONCEPT_ID = 5096
DAYS_BEFORE_LTFU: Final[int] = 28


pmtct_hts_register_concepts = {
    "Form_ID": 54,
    "Weight loss": 832,
    "Night sweats": 133027,
    "Fever": 140238,
    "Cough": 143264,
    "Result of HIV test": 159427,
    "HIV Test accepted": 164167,
    "Received HIV test result": 164848,
    "Agreed To Partner Notification": 164954,
    "TB Screening Score": 165808,
    "HTS Register Date": 166029,
    "Previously Known HIV Positive Result": 166030
}

general_antenatal_care_concepts = {
    "Form_ID": 16,
    "VDRL": 299,
    "Date Tested For Syphilis": 164952,
    "Tested for Syphilis": 165280,
    "General Antenatal Number": 165567
}

ctd_concepts = {
    "Form_ID": 13,
    "Reason for Tracking": 165460,
    "Guardian / Treatment Partner's Name": 161135,
    "Guardian / Treatment Partner's Contact Address": 160641,
    "Guardian / Treatment Partner's Phone Number": 159635,
    "Date of Last Actual Contact/ Appointment": 165461,
    "Date of Missed Scheduled Appointment": 165778,
    "Client Verification": 167221,
    "Indication for Client Verification": 167222,
    "Patient Care in Facility Discontinued": 165586,
    "Date of Discontinuation": 165469,
    "Reason for Discontinuation": 165470,
    "Facility transferred to": 159495,
    "Cause of Death": 165889,
    "VA Cause of Death": 166349,
    "Adult Causes": 166348,
    "Child Causes": 166347,
    "Other cause of death": 165915,
    "Reason to Discontinue Care": 165916,
    "Discontinue Care other specify": 165917,
    "Date of Lost to follow up": 166152,
    "Reason for Lost to follow up": 166157,
    "Reason for Lost to follow up_Other": 167149,
}

# Popular Identifiers
PEPFAR_UNIQUE_ID = 4
HOSPITAL_UNIQUE_ID = 3

# Popular PMM Forms
ART_COMMENCEMENT_FORM_ID = 56
HIV_ENROLLMENT_FORM_ID = 23
CARE_CARD_FORM_ID = 14
PHARMACY_FORM_ID = 27
CLIENT_TRACKING_DISCONTINUATION_FORM_ID = 13
EAC_FORM_ID = 69
LAB_FORM_ID = 21

PMTCT_HTS_REGISTER_FORM_ID = pmtct_hts_register_concepts.get("Form_ID")

# Popular Analysis Period 
FY25_START_DATE: Final[datetime] = datetime(2024, 10, 1, 0, 0, 0)
FY25_END_DATE: Final[datetime] = datetime(2025, 9, 30, 23, 59, 59)

FY26_START_DATE: Final[datetime] = datetime(2025, 10, 1, 0, 0, 0)
FY26_END_DATE: Final[datetime] = datetime(2026, 9, 30, 23, 59, 59)

