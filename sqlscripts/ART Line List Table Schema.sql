CREATE TABLE art_line_list (
    -- Location & Facility Info
	recordid SERIAL PRIMARY KEY,
	patientuuid UUID UNIQUE,
	cuttoffperiod TIMESTAMP,
	touchtime TIMESTAMP,
    state VARCHAR(100),
    lga VARCHAR(100),
    datimcode VARCHAR(50),
    facilityname VARCHAR(255),
    
    -- Patient Identifiers
    patientuniqueid VARCHAR(100),
    patienthospitalno VARCHAR(100),
    ancnoidentifier VARCHAR(100),
    ancnoconceptid VARCHAR(100),
    htsno VARCHAR(100),
    sex VARCHAR(10),
    
    -- ART Start Details
    ageatstartofartyears INTEGER,
    ageatstartofartmonths INTEGER,
    careentrypoint VARCHAR(100),
    hivconfirmeddate TIMESTAMP,
    monthsonart INTEGER,
    datetransferredin TIMESTAMP,
    transferinstatus VARCHAR(100),
    artstartdate TIMESTAMP,
    
    -- Visit Details
    lastpickupdate TIMESTAMP,
    lastvisitdate TIMESTAMP,
    daysofarvrefil INTEGER,
    pillbalance NUMERIC(10, 2),
    
    -- Regimen Details
    initialregimenline VARCHAR(100),
    initialregimen VARCHAR(100),
    initialcd4count NUMERIC(10, 2),
    initialcd4countdate TIMESTAMP,
    currentcd4count NUMERIC(10, 2),
    currentcd4countdate TIMESTAMP,
    lasteacdate TIMESTAMP,
    currentregimenline VARCHAR(100),
    currentregimen VARCHAR(100),
    
    -- Pregnancy / Maternal
    pregnancystatus VARCHAR(50),
    pregnancystatusdate TIMESTAMP,
    edd TIMESTAMP,
    lastdeliverydate TIMESTAMP,
    lmp TIMESTAMP,
    gestationageweeks INTEGER,
    
    -- Viral Load Data
    currentviralload NUMERIC(15, 2),
    viralloadencounterdate TIMESTAMP,
    viralloadsamplecollectiondate TIMESTAMP,
    viralloadreporteddate TIMESTAMP,
    resultdate TIMESTAMP,
    assaydate TIMESTAMP,
    approvaldate TIMESTAMP,
    viralloadindication VARCHAR(255),
    
    -- Outcomes & Dispensing
    patientoutcome VARCHAR(100),
    patientoutcomedate TIMESTAMP,
    currentartstatus VARCHAR(100),
    dispensingmodality VARCHAR(100),
    facilitydispensingmodality VARCHAR(100),
    ddddispensingmodality VARCHAR(100),
    mmdtype VARCHAR(100),
    datereturnedtocare TIMESTAMP,
    dateoftermination TIMESTAMP,
    pharmacynextappointment TIMESTAMP,
    clinicalnextappointment TIMESTAMP,
    
    -- Current Demographics
    currentageyears INTEGER,
    currentagemonths INTEGER,
    dateofbirth TIMESTAMP,
    markasdeseased BOOLEAN DEFAULT FALSE,
    markasdeseaseddeathdate TIMESTAMP,
    
    -- Contacts
    registrationphoneno VARCHAR(50),
    nextofkinphoneno VARCHAR(50),
    treatmentsupporterphoneno VARCHAR(50),
    
    -- Biometrics
    biometriccaptured VARCHAR(10), -- Yes/No
    biometriccapturedate TIMESTAMP,
    validcapture VARCHAR(10),
    
    -- Vitals & TB Status
    currentweight_kg NUMERIC(10, 2),
    currentweightdate TIMESTAMP,
    tbstatus VARCHAR(255),
    tbstatusdate TIMESTAMP,
    baselineinhstartdate TIMESTAMP,
    baselineinhstopdate TIMESTAMP,
    currentinhstartdate TIMESTAMP,
    currentinhoutcome VARCHAR(100),
    currentinhoutcomedate TIMESTAMP,
    lastinhdispenseddate TIMESTAMP,
    baselinetbtreatmentstartdate TIMESTAMP,
    baselinetbtreatmentstopdate TIMESTAMP,
    
    -- Lab History
    lastviralloadsamplecollectionformdate TIMESTAMP,
    lastsampletakendate TIMESTAMP,
    otzenrollmentdate TIMESTAMP,
    otzoutcomedate TIMESTAMP,
    enrollmentdate TIMESTAMP,
    
    -- Line Regimen History
    initialfirstlineregimen VARCHAR(100),
    initialfirstlineregimendate TIMESTAMP,
    initialsecondlineregimen VARCHAR(100),
    initialsecondlineregimendate TIMESTAMP,
    
    -- Previous Quarter Reporting
    lastpickupdatepreviousquarter TIMESTAMP,
    drugdurationpreviousquarter INTEGER,
    patientoutcomepreviousquarter VARCHAR(100),
    patientoutcomedatepreviousquarter TIMESTAMP,
    artstatuspreviousquarter VARCHAR(100),
    
    -- Visit Details Continued
    quantityofarvdispensedlastvisit INTEGER,
    frequencyofarvdispensedlastvisit VARCHAR(100),
    currentartstatuswithpillbalance VARCHAR(100),
    
    -- Recapture & Screening
    recapturedate TIMESTAMP,
    recapturecount INTEGER,
    cervicalcancerscreeningstatus VARCHAR(100),
    cervicalcancerscreeningstatusdate TIMESTAMP,
    cervicalcancertreatmentprovided VARCHAR(100),
    cervicalcancertreatmentprovideddate TIMESTAMP
);