-- Drop tables if they exist to allow clean, repeatable testing
DROP TABLE IF EXISTS reactions;
DROP TABLE IF EXISTS drugs;
DROP TABLE IF EXISTS reports;

-- Create Parent Reports Table (1 row per unique adverse event)
CREATE TABLE reports (
    safetyreportid TEXT PRIMARY KEY,
    safetyreportversion INTEGER,
    receivedate DATE,
    transmissiondate DATE,
    primarysourcecountry TEXT,
    occurcountry TEXT,
    reporttype INTEGER,
    serious INTEGER,
    seriousnessdeath INTEGER,
    seriousnessdisabling INTEGER,
    seriousnessother INTEGER,
    patient_onset_age NUMERIC,
    patient_onset_age_unit INTEGER,
    patient_sex INTEGER,
    patient_death_date DATE
);

-- Create Child Drugs Table (Can have multiple rows pointing to 1 report)
CREATE TABLE drugs (
    drug_id SERIAL PRIMARY KEY,
    safetyreportid TEXT REFERENCES reports(safetyreportid) ON DELETE CASCADE,
    medicinal_product TEXT,
    drug_characterization INTEGER
);

-- Create Child Reactions Table (Can have multiple side effects pointing to 1 report)
CREATE TABLE reactions (
    reaction_id SERIAL PRIMARY KEY,
    safetyreportid TEXT REFERENCES reports(safetyreportid) ON DELETE CASCADE,
    reaction_meddra_pt TEXT
);