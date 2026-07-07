import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
from collections import OrderedDict
import os
import logging
import dao.mongodbdao as mongo_dao
import utils.demographicutils as demographicsutils
import formslib.artcommencementutil as artcommence
import formslib.hivenrollmentutil as hivenrollmentutils
import formslib.carecardutils as carecardutils
import formslib.pharmacyutils as pharmacyutils
import utils.encounterutils as encounterutils
import formslib.labutils as labutils
import formslib.eacutils as eacutils
import utils.obsutils as obsutils
import formslib.ctdutils as ctdutils
import utils.commonutils as commonutils
import dao.postgresdao as postgres_dao
import dao.postgresquarterupsert as postgres_upsert
from dao.config import MONGO_DATABASE_NAME, EAC_TABLE_NAME


# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}
ASPIRE_STATES = {"FCT", "KATSINA", "NASARAWA", "RIVERS"}




def convert_doc_to_record(doc, cutoff_datetime):
    """
    Converts a MongoDB document to a dictionary record for PostgreSQL insertion.
    This function is called for each document in the cursor.
    """
    # Extract necessary fields from the document using utility functions
    # and construct the record dictionary as shown in the main loop.
    # For brevity, this function is not fully implemented here, but it would
    # contain the same logic as the record construction in the main loop of initialize_art_line_list_data.
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    start_datetime = commonutils.normalize_clinical_date(datetime(2024, 10, 1))
    # end_datetime = commonutils.normalize_clinical_date(datetime.now())
    end_datetime = commonutils.normalize_clinical_date(datetime.now())


    header = demographicsutils.get_message_header(doc)
    datim_code = header.get("facilityDatimCode")
    demographics = demographicsutils.get_patient_demographics(doc)
    birthdate = commonutils.normalize_clinical_date(demographics.get("birthdate"))
    facility_info = get_facility_by_datim(datim_code)
    art_start_date = commonutils.normalize_clinical_date(artcommence.get_art_start_date(doc, cutoff_datetime))
    eac_1_date = commonutils.normalize_clinical_date(eacutils.get_eac_date(1, doc))
    last_eac_encounter=eacutils.get_last_eac_encounter(doc,cutoff_datetime)
    viral_load_before_first_eac_obs = labutils.get_last_viral_load_obs_before(doc, eac_1_date)
    viral_load_1_obs = labutils.get_nth_viral_load_obs(doc, 1, cutoff_datetime)
    viral_load_2_obs = labutils.get_nth_viral_load_obs(doc, 2, cutoff_datetime)
    viral_load_3_obs = labutils.get_nth_viral_load_obs(doc, 3, cutoff_datetime)
    #viral_load_1_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_load_obs(doc, 1, 3,cutoff_datetime)
    #viral_load_2_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_load_obs(doc, 2, 3,cutoff_datetime)
    #viral_load_3_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_load_obs(doc, 3, 3,cutoff_datetime)

    current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime)
    #current_viral_load_obs = labutils.get_first_unsuppressed_viral_load_between_dates(doc, start_datetime, end_datetime)
    current_viral_load_obsdatetime = obsutils.getObsDatetimeFromObs(current_viral_load_obs) if current_viral_load_obs else None
    last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)
    current_pregnancy_status_obs=carecardutils.get_current_pregnancy_status_obs(doc,cutoff_datetime)
    first_unsuppressed_viral_load_obs = labutils.get_first_unsuppressed_viral_load_between_dates(doc, start_datetime, end_datetime)
    first_unsuppressed_viral_load_value = obsutils.getValueNumericFromObs(first_unsuppressed_viral_load_obs) if first_unsuppressed_viral_load_obs else None
    first_unsuppressed_viral_load_datetime = obsutils.getObsDatetimeFromObs(first_unsuppressed_viral_load_obs) if first_unsuppressed_viral_load_obs else None
    last_eac_encounter_datetime = encounterutils.get_encounter_datetime(last_eac_encounter) if last_eac_encounter else None
    viral_load_after_last_eac_obs = labutils.get_first_viral_load_after_date(doc, last_eac_encounter_datetime) if last_eac_encounter_datetime else None
    viral_load_after_last_eac_value = obsutils.getValueNumericFromObs(viral_load_after_last_eac_obs) if viral_load_after_last_eac_obs else None
    viral_load_after_last_eac_datetime = obsutils.getObsDatetimeFromObs(viral_load_after_last_eac_obs) if viral_load_after_last_eac_obs else None
    regimen_after_last_eac_value = obsutils.getValueNumericFromObs(viral_load_after_last_eac_obs) if viral_load_after_last_eac_obs else None
    regimen_after_last_eac_datetime = obsutils.getObsDatetimeFromObs(viral_load_after_last_eac_obs) if viral_load_after_last_eac_obs else None


    record = {

        "touchtime": header.get("touchTime"),
        "State": facility_info.get("State") if facility_info else None,
        "LGA" : facility_info.get("LGA") if facility_info else None,
        "DatimCode" : header.get("facilityDatimCode"),
        "FacilityName": header.get("facilityName"),
        "UniqueID": demographicsutils.get_patient_identifier(4, doc),
        "HospitalNumber": demographicsutils.get_patient_identifier(5, doc),
        "Sex": demographics.get("gender"),
        "AgeAtARTStartYears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
        "AgeAtARTStartMonths": demographicsutils.get_pediatric_age_art_start_months(doc, birthdate, art_start_date),
        "CurrentAgeYears": demographicsutils.get_current_age_at_date(doc,cutoff_datetime),
        "CurrentAgeMonths": demographicsutils.get_current_age_at_date_in_months(doc,cutoff_datetime),
        "DOB": birthdate,
        "CareEntryPoint": hivenrollmentutils.get_care_entry_point(doc,cutoff_datetime),
        "MonthsOnArt": demographicsutils.get_months_on_art(doc,art_start_date,cutoff_datetime),
        "DateTransferredIn": hivenrollmentutils.get_date_transferred_in(doc,cutoff_datetime),
        "TransferredInStatus": hivenrollmentutils.get_prior_art(doc,cutoff_datetime),
        "ArtStartDate": art_start_date,
        "LastPickupDate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
        "LastVisitDate": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
        "DaysOfARVRefill": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
        "PillBalance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),
        "PatientOutcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime),
        "PatientOutcomeDate" : ctdutils.get_outcome_date (doc,cutoff_datetime),
        "CurrentArtStatus": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
        "DispensingModality": pharmacyutils.get_last_dsd_model(doc,cutoff_datetime),
        "FacilityDispensingModality": pharmacyutils.get_facility_dsd_model(doc,cutoff_datetime),
        "DDDDispensingModality": pharmacyutils.get_ddd_dsd_model(doc,cutoff_datetime),
        "MMDType": pharmacyutils.get_mmd_type(doc,cutoff_datetime),
        "PharmacyNextAppointmentDate": pharmacyutils.get_pharmacy_next_appointment_date(doc, cutoff_datetime),
        "ClinicalNextAppointmentDate": carecardutils.get_clinical_next_appointment_date(doc,cutoff_datetime),
        "CurrentViralLoad": obsutils.getValueNumericFromObs(current_viral_load_obs),
        "ViralLoadEncounterDate": obsutils.getObsDatetimeFromObs(current_viral_load_obs),
        "ViralLoadSampleDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, current_viral_load_obs)),
        "ViralLoadIndication": obsutils.getVariableValueFromObs(labutils.get_viral_load_indication_obs_of_viral_load_obs(doc, current_viral_load_obs)),
        "LastSampleTakenDate": obsutils.getValueDatetimeFromObs(labutils.get_last_sample_taken_date_obs(doc,cutoff_datetime)),
        "ViralLoadBefore1stEAC": obsutils.getValueNumericFromObs(viral_load_before_first_eac_obs),
        "ViralLoadBefore1stEACDate": obsutils.getObsDatetimeFromObs(viral_load_before_first_eac_obs),
        "ViralLoadBefore1stEACSampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc,viral_load_before_first_eac_obs)),
        "ViralLoadBefore1stEACReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc,viral_load_before_first_eac_obs)),
        "EAC1date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,1,current_viral_load_obsdatetime)),
        "EAC2date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,2,current_viral_load_obsdatetime)),
        "EAC3date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,3,current_viral_load_obsdatetime)),
        "EAC4date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,4,current_viral_load_obsdatetime)),
        "EAC5date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,5,current_viral_load_obsdatetime)),
        "EAC6date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,6,current_viral_load_obsdatetime)),
        "EAC7date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,7,current_viral_load_obsdatetime)),
        "EAC8date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,8,current_viral_load_obsdatetime)),
        "ViralLoad1": obsutils.getValueNumericFromObs(viral_load_1_obs),
        "ViralLoad1ReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_1_obs)),
        "ViralLoad1SampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_1_obs)),
        "ViralLoad2": obsutils.getValueNumericFromObs(viral_load_2_obs),
        "ViralLoad2ReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_2_obs)),
        "ViralLoad2SampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_2_obs)),
        "ViralLoad3": obsutils.getValueNumericFromObs(viral_load_3_obs),
        "ViralLoad3ReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_3_obs)),
        "ViralLoad3SampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_3_obs)),
        "CurrentRegimenLine": pharmacyutils.get_current_regimen_line(doc,cutoff_datetime) ,
        "CurrentRegimen": pharmacyutils.get_current_regimen(doc,cutoff_datetime),
        "SecondLineRegimenStartDate": pharmacyutils.get_min_second_line_regimen_date(doc,cutoff_datetime),
        "ThirdLineRegimenStartDate": pharmacyutils.get_min_third_line_regimen_date(doc,cutoff_datetime),
        "CurrentPregnancyStatus": obsutils.getVariableValueFromObs(current_pregnancy_status_obs),
        "CurrentPregnancyStatusDatetime": obsutils.getObsDatetimeFromObs(current_pregnancy_status_obs),
        "EDD": obsutils.getValueDatetimeFromObs(carecardutils.get_edd_for_last_pregnancy(doc,current_pregnancy_status_obs)),
        "LastEACSessionType": eacutils.get_last_eac_session_type(doc,last_eac_encounter,cutoff_datetime),
        "LastEACSessionDate": encounterutils.get_encounter_datetime (last_eac_encounter),
        "LastEACBarriersToAdherence": eacutils.get_last_eac_barriers_to_adherence(doc,last_eac_encounter, cutoff_datetime),
        "LastEACRegimenPlan": eacutils.get_last_eac_regimen_plan(doc, last_eac_encounter, cutoff_datetime),
        "LastEACFollowupDate": eacutils.get_last_eac_followup_date(doc, last_eac_encounter, cutoff_datetime),
        "LastEACAdherenceComments": eacutils.get_last_eac_comments(doc, last_eac_encounter, cutoff_datetime),
        "LastEACReferral": eacutils.get_eac_referral(doc, last_eac_encounter, cutoff_datetime),
        "LastReferralSwitchCommitteeDate": eacutils.get_referral_switch_commitee_date(doc, last_eac_encounter, cutoff_datetime),
        "PatientUUID": demographicsutils.get_patient_demographics(doc).get("patientUuid"),
        "Quarter": commonutils.get_fy_and_quarter_from_date(obsutils.getObsDatetimeFromObs(current_viral_load_obs)), # type: ignore
        "firstUnsuppressedViralLoad": first_unsuppressed_viral_load_value,
        "firstUnsuppressedViralLoadDate": first_unsuppressed_viral_load_datetime,
        "viralLoadAfterLastEAC": viral_load_after_last_eac_value,
        "viralLoadAfterLastEACDate": viral_load_after_last_eac_datetime,
        "regimenAfterEAC": regimen_after_last_eac_value,
        "regimenAfterEACDate": regimen_after_last_eac_datetime

    }
    return record


def load_facility_cache(db, db_name=MONGO_DATABASE_NAME):
    """
    Loads all facilities into a dictionary indexed by DATIM code.
    Run this once at the start of your ETL.
    """
    global _facility_cache
    facilities = mongo_dao.get_all_facilities(db, db_name)
    _facility_cache = {f.get("DATIM"): f for f in facilities if f.get("DATIM")}
    print(f"Loaded {len(_facility_cache)} facilities into memory cache.")


def get_facility_by_datim(datim_code):
    """
    Returns the full facility JSON for a given DATIM code.
    Returns None if the code is not found.
    """
    return _facility_cache.get(datim_code)


# check if document belongs to a facility in ASPIRE states (FCT,Katsina,Nasarawa,Rivers) ignore casing and whitespace
def is_aspire_state(doc):
    header = demographicsutils.get_message_header(doc)
    datim_code = header.get("facilityDatimCode")

    if not datim_code:
        return False
    facility = get_facility_by_datim(datim_code)

    if facility is None:
        return False
    if facility:
        state = facility.get("State", "").strip().upper()
        return state in ASPIRE_STATES
    return False


def export_eac_data_to_postgresql(
    cutoff_datetime=None,
    table_name=EAC_TABLE_NAME,
    batch_size=5000,
    filter_aspire_only=False
):
    """
    Exports EAC data from MongoDB to PostgreSQL with quarter-based upsert.
    
    Logic:
    - If record doesn't exist (by patientuuid + quarter): INSERT
    - If record exists with same quarter:
      - If new touchtime > DB touchtime: UPDATE
      - If new touchtime <= DB touchtime: SKIP
    - If quarter is different: INSERT as new record
    
    Args:
        cutoff_datetime: Cutoff date for data extraction (default: now)
        table_name: PostgreSQL table name (default: eac_line_list)
        batch_size: Number of records to batch before upserting (default: 5000)
        filter_aspire_only: If True, only process ASPIRE state facilities (default: False)
    
    Returns:
        Dictionary: {"total_processed": int, "inserted": int, "updated": int, "skipped": int}
    """
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('eac_pg_export.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    
    logger.info(f"Starting EAC data export to PostgreSQL")
    logger.info(f"Cutoff datetime: {cutoff_datetime}")
    logger.info(f"Target table: {table_name}")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Filter ASPIRE only: {filter_aspire_only}")
    
    try:
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        mongo_db = mongo_dao.get_db_connection(MONGO_DATABASE_NAME)
        if mongo_db is None:
            logger.error("Failed to connect to MongoDB")
            return {"total_processed": 0, "inserted": 0, "updated": 0, "skipped": 0}
        
        # Load facility cache for O(1) lookups
        logger.info("Loading facility cache...")
        load_facility_cache(mongo_db, MONGO_DATABASE_NAME)
        
        # Connect to PostgreSQL
        logger.info("Connecting to PostgreSQL...")
        pg_conn = postgres_dao.connect_to_postgresqldb()
        if pg_conn is None:
            logger.error("Failed to connect to PostgreSQL")
            return {"total_processed": 0, "inserted": 0, "updated": 0, "skipped": 0}

        # Ensure ON CONFLICT key has a unique index before running long ETL batches.
        try:
            logger.info(f"[PREFLIGHT] Ensuring unique index on {table_name}(patientuuid, quarter)...")
            postgres_upsert.ensure_quarter_upsert_index(pg_conn, table_name, auto_create=True)
            logger.info(f"[PREFLIGHT] Verified unique index for upsert key (patientuuid, quarter) on {table_name}")
        except Exception as e:
            logger.error(f"[PREFLIGHT FAILED] {e}")
            logger.error(f"Upsert preflight failed for table {table_name}: {e}")
            if pg_conn is not None:
                pg_conn.close()
            return {
                "total_processed": 0,
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "errors": 1
            }

        # Get cursor for EAC containers
        logger.info("Fetching EAC containers from MongoDB...")
        cursor, total_size = mongo_dao.get_art_containers(mongo_db, MONGO_DATABASE_NAME)
        
        logger.info(f"Found {total_size} total EAC containers")
        
        # Process records in batches
        batch_records = []
        total_stats = {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
        batch_number = 0
        
        logger.info("Processing records...")
        
        for doc in tqdm(cursor, total=total_size, desc="EAC Export Progress"):
            try:
                # Filter ASPIRE states if requested
                if filter_aspire_only and not is_aspire_state(doc):
                    continue
                
                # Convert MongoDB document to PostgreSQL record
                record = convert_doc_to_record(doc, cutoff_datetime)
                
                # Validate required fields for upsert
                if not record.get("PatientUUID") or not record.get("Quarter"):
                    logger.debug(f"Skipping record: missing PatientUUID or Quarter")
                    continue
                
                # Ensure touchtime is set
                if not record.get("touchtime"):
                    record["touchtime"] = datetime.now()
                
                batch_records.append(record)
                
                # Upsert batch when it reaches batch_size
                if len(batch_records) >= batch_size:
                    batch_number += 1
                    batch_stats = _upsert_batch(
                        pg_conn,
                        table_name,
                        batch_records,
                        batch_number,
                        logger
                    )
                    
                    # Accumulate stats
                    total_stats["inserted"] += batch_stats.get("inserted", 0)
                    total_stats["updated"] += batch_stats.get("updated", 0)
                    total_stats["skipped"] += batch_stats.get("skipped", 0)
                    total_stats["errors"] += batch_stats.get("errors", 0)
                    
                    batch_records = []
            
            except Exception as e:
                logger.error(f"Error processing document: {e}")
                total_stats["errors"] += 1
                continue
        
        # Upsert remaining records
        if batch_records:
            batch_number += 1
            logger.info(f"Upserting final batch ({len(batch_records)} records)")
            batch_stats = _upsert_batch(
                pg_conn,
                table_name,
                batch_records,
                batch_number,
                logger
            )
            
            total_stats["inserted"] += batch_stats.get("inserted", 0)
            total_stats["updated"] += batch_stats.get("updated", 0)
            total_stats["skipped"] += batch_stats.get("skipped", 0)
            total_stats["errors"] += batch_stats.get("errors", 0)
        
        # Final statistics
        total_processed = total_stats["inserted"] + total_stats["updated"] + total_stats["skipped"]
        
        logger.info("\n" + "="*70)
        logger.info("EAC EXPORT COMPLETED")
        logger.info("="*70)
        logger.info(f"Total records processed: {total_processed}")
        logger.info(f"  [OK] Inserted: {total_stats['inserted']}")
        logger.info(f"  [OK] Updated: {total_stats['updated']}")
        logger.info(f"  [OK] Skipped: {total_stats['skipped']}")
        logger.info(f"  [ERROR] Errors: {total_stats['errors']}")

        if total_processed > 0:
            success_rate = (total_stats["inserted"] + total_stats["updated"]) / total_processed * 100
            logger.info(f"Success rate: {success_rate:.1f}%")
        
        logger.info("="*70)
        
        # Close connections
        if pg_conn is not None:
            pg_conn.close()
            logger.info("PostgreSQL connection closed")
        
        return {
            "total_processed": total_processed,
            "inserted": total_stats["inserted"],
            "updated": total_stats["updated"],
            "skipped": total_stats["skipped"],
            "errors": total_stats["errors"]
        }
    
    except Exception as e:
        logger.error(f"Fatal error during export: {e}", exc_info=True)
        return {"total_processed": 0, "inserted": 0, "updated": 0, "skipped": 0, "errors": 1}


def _upsert_batch(pg_conn, table_name, records, batch_number, logger):
    """
    Helper function to upsert a batch of records to PostgreSQL.
    
    Args:
        pg_conn: PostgreSQL connection
        table_name: Target table name
        records: List of records to upsert
        batch_number: Batch number for logging
        logger: Logger instance
    
    Returns:
        Dictionary with upsert results
    """
    try:
        logger.info(f"Upserting batch {batch_number} ({len(records)} records) to {table_name}...")
        
        result = postgres_upsert.batch_upsert_by_quarter(
            pg_conn,
            table_name,
            records,
            protected_keys={'recordid', 'patientuuid', 'quarter'}
        )
        
        logger.info(f"Batch {batch_number} result: "
                   f"{result['inserted']} inserted, "
                   f"{result['updated']} updated, "
                   f"{result['skipped']} skipped")
        
        return result
    
    except Exception as e:
        logger.error(f"Error upserting batch {batch_number}: {e}")
        return {"inserted": 0, "updated": 0, "skipped": 0, "errors": len(records)}


if __name__ == "__main__":
    """
    Main execution block for EAC PostgreSQL export.
    
    Usage:
        python eacPgExport.py
    
    Customize the export by modifying arguments:
        export_eac_data_to_postgresql(
            cutoff_datetime=datetime(2024, 1, 1),
            table_name=EAC_TABLE_NAME,
            batch_size=5000,
            filter_aspire_only=False
        )
    """
    import sys
    
    try:
        # Run export with default settings
        result = export_eac_data_to_postgresql(
            cutoff_datetime=None,  # Use current date
            table_name=EAC_TABLE_NAME,
            batch_size=5000,
            filter_aspire_only=False
        )
        
        # Exit with status code
        sys.exit(0 if result["errors"] == 0 else 1)
    
    except KeyboardInterrupt:
        print("\n\nExport cancelled by user")
        sys.exit(1)
    
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)



