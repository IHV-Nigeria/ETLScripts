CREATE TABLE eac_line_list (
    id SERIAL,

    touchtime TIMESTAMP,
    state TEXT,
    lga TEXT,
    datimcode TEXT,
    facilityname TEXT,

    uniqueid TEXT,
    hospitalnumber TEXT,
    sex TEXT,

    ageatartstartyears INTEGER,
    ageatartstartmonths INTEGER,
    currentageyears INTEGER,
    currentagemonths INTEGER,
    dob TIMESTAMP,

    careentrypoint TEXT,
    monthsonart INTEGER,
    datetransferredin TIMESTAMP,
    transferredinstatus TEXT,
    artstartdate TIMESTAMP,

    lastpickupdate TIMESTAMP,
    lastvisitdate TIMESTAMP,
    daysofarvrefill NUMERIC,
    pillbalance NUMERIC,

    patientoutcome TEXT,
    patientoutcomedate TIMESTAMP,
    currentartstatus TEXT,

    dispensingmodality TEXT,
    facilitydispensingmodality TEXT,
    ddddispensingmodality TEXT,
    mmdtype TEXT,

    pharmacynextappointmentdate TIMESTAMP,
    clinicalnextappointmentdate TIMESTAMP,

    currentviralload NUMERIC,
    viralloadencounterdate TIMESTAMP,
    viralloadsampledate TIMESTAMP,
    viralloadindication TEXT,
    lastsampletakendate TIMESTAMP,

    viralloadbefore1steac NUMERIC,
    viralloadbefore1steacdate TIMESTAMP,
    viralloadbefore1steacsamplecollectiondate TIMESTAMP,
    viralloadbefore1steacreporteddate TIMESTAMP,

    eac1date TIMESTAMP,
    eac2date TIMESTAMP,
    eac3date TIMESTAMP,
    eac4date TIMESTAMP,
    eac5date TIMESTAMP,
    eac6date TIMESTAMP,
    eac7date TIMESTAMP,
    eac8date TIMESTAMP,

    viralload1 NUMERIC,
    viralload1reporteddate TIMESTAMP,
    viralload1samplecollectiondate TIMESTAMP,

    viralload2 NUMERIC,
    viralload2reporteddate TIMESTAMP,
    viralload2samplecollectiondate TIMESTAMP,

    viralload3 NUMERIC,
    viralload3reporteddate TIMESTAMP,
    viralload3samplecollectiondate TIMESTAMP,

    currentregimenline TEXT,
    currentregimen TEXT,
    secondlineregimenstartdate TIMESTAMP,
    thirdlineregimenstartdate TIMESTAMP,

    currentpregnancystatus TEXT,
    currentpregnancystatusdatetime TIMESTAMP,
    edd TIMESTAMP,

    lasteacsessiontype TEXT,
    lasteacsessiondate TIMESTAMP,
    lasteacbarrierstoadherence TEXT,
    lasteacregimenplan TEXT,
    lasteacfollowupdate TIMESTAMP,
    lasteacadherencecomments TEXT,
    lasteacreferral TEXT,
    lastreferralswitchcommitteedate TIMESTAMP,

    patientuuid TEXT PRIMARY KEY,
    quarter TEXT,

    firstunsuppressedviralload NUMERIC,
    firstunsuppressedviralloaddate TIMESTAMP,

    viralloadafterlasteac NUMERIC,
    viralloadafterlasteacdate TIMESTAMP,

    regimenaftereac TEXT,
    regimenaftereacdate TIMESTAMP
);