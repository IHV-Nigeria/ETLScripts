BEGIN;

-- Optional: create the sequence referenced in your default
-- (kept minimal; adjust START/CACHE if you have specific requirements)
CREATE SEQUENCE IF NOT EXISTS eac_line_list_id_seq
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Create table (in public schema; change if needed)
CREATE TABLE IF NOT EXISTS public.eac_line_list (
    id BIGINT PRIMARY KEY NOT NULL
        DEFAULT nextval('eac_line_list_id_seq'::regclass),

    create_date TIMESTAMP WITHOUT TIME ZONE,
    update_date TIMESTAMP WITHOUT TIME ZONE,

    state VARCHAR(255),
    lga VARCHAR(255),
    datim_code VARCHAR(255),
    facility_name VARCHAR(255),
    patient_unique_id VARCHAR(255),
    patient_hospital_no VARCHAR(255),
    sex VARCHAR(255),

    age_at_start_of_art_years INTEGER,
    age_at_start_of_art_months INTEGER,
    care_entry_point VARCHAR(255),
    kp_type VARCHAR(255),
    months_on_art INTEGER,
    date_transferred_in DATE,
    transfer_in_status VARCHAR(255),

    art_start_date DATE,
    last_pickup_date DATE,
    last_visit_date DATE,
    days_of_arv_refill INTEGER,
    pill_balance INTEGER,

    viral_load_before_first_eac DOUBLE PRECISION,
    viral_load_sample_collection_date_before_first_eac DATE,
    date_result_was_received_in_facility_before_first_eac DATE,

    eac1date DATE,
    eac2date DATE,
    eac3date DATE,
    eac4date DATE,
    eac5date DATE,
    eac6date DATE,
    eac7date DATE,
    eac8date DATE,

    viral_load1 DOUBLE PRECISION,
    viral_load2 DOUBLE PRECISION,
    viral_load3 DOUBLE PRECISION,
    viral_load1sample_date DATE,
    viral_load2sample_date DATE,
    viral_load3sample_date DATE,
    viral_load1report_date DATE,
    viral_load2report_date DATE,
    viral_load3report_date DATE,

    current_regimen_line VARCHAR(255),
    current_regimen VARCHAR(255),

    second_line_regimen_start_date DATE,
    third_line_regimen_start_date DATE,

    pregnancy_status VARCHAR(255),
    pregnancy_status_date DATE,
    edd DATE,

    last_eac_session_type VARCHAR(255),
    last_eac_session_date DATE,
    last_eac_barriers_to_adherence VARCHAR(255),
    last_eac_regimen_plan VARCHAR(255),
    last_eac_followup_date DATE,
    last_eac_adherence_counsellor_comments TEXT,

    current_viral_load DOUBLE PRECISION,
    viral_load_encounter_date DATE,
    viral_load_sample_collection_date DATE,
    viral_load_indication VARCHAR(255),
    last_sample_taken_date DATE,

    patient_outcome VARCHAR(255),
    patient_outcome_date DATE,
    current_art_status VARCHAR(255),

    dispensing_modality VARCHAR(255),
    facility_dispensing_modality VARCHAR(255),
    ddd_dispensing_modality VARCHAR(255),
    mmd_type VARCHAR(255),

    pharmacy_next_appointment_date DATE,
    clinical_next_appointment_date DATE,

    current_age_years INTEGER,
    current_age_months INTEGER,
    date_of_birth DATE,

    patient_uuid VARCHAR(255),
    quarter VARCHAR(255),

    -- Alternate EAC-series columns you also provided later
    eac_1_date DATE,
    eac_2_date DATE,
    eac_3_date DATE,
    eac_4_date DATE,
    eac_5_date DATE,
    eac_6_date DATE,
    eac_7_date DATE,
    eac_8_date DATE,

    viral_load_1 DOUBLE PRECISION,
    viral_load_1_report_date DATE,
    viral_load_1_sample_collection_date DATE,

    viral_load_2 DOUBLE PRECISION,
    viral_load_2_report_date DATE,
    viral_load_2_sample_collection_date DATE,

    viral_load_3 DOUBLE PRECISION,
    viral_load_3_report_date DATE,
    viral_load_3_sample_collection_date DATE
);

COMMIT;