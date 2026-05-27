WITH raw_reports AS (
    SELECT * FROM "airflow"."public"."reports"
)

SELECT
    safetyreportid,
    safetyreportversion,
    receivedate,
    transmissiondate,
    primarysourcecountry,
    occurcountry,
    
    -- Decode reporttype strictly per your project dictionary
    CASE 
        WHEN reporttype = 1 THEN 'Spontaneous'
        WHEN reporttype = 2 THEN 'Report from study'
        WHEN reporttype = 3 THEN 'Other'
        WHEN reporttype = 4 THEN 'Not available to sender (unknown)'
        ELSE 'Unspecified'
    END AS reporttype,

    -- Decode serious in place
    CASE 
        WHEN serious = 1 THEN 'Yes'
        WHEN serious = 2 THEN 'No'
        ELSE 'Unspecified'
    END AS serious,

    -- Decode seriousness outcome flags in place
    CASE WHEN seriousnessdeath = 1 THEN 'Yes' WHEN seriousnessdeath = 2 THEN 'No' ELSE 'No' END AS seriousnessdeath,
    CASE WHEN seriousnessdisabling = 1 THEN 'Yes' WHEN seriousnessdisabling = 2 THEN 'No' ELSE 'No' END AS seriousnessdisabling,
    CASE WHEN seriousnessother = 1 THEN 'Yes' WHEN seriousnessother = 2 THEN 'No' ELSE 'No' END AS seriousnessother,

    patient_onset_age,
    
    -- Decode patient_onset_age_unit in place
    CASE 
        WHEN patient_onset_age_unit = 800 THEN 'Decade'
        WHEN patient_onset_age_unit = 801 THEN 'Year'
        WHEN patient_onset_age_unit = 802 THEN 'Month'
        WHEN patient_onset_age_unit = 803 THEN 'Week'
        WHEN patient_onset_age_unit = 804 THEN 'Day'
        WHEN patient_onset_age_unit = 805 THEN 'Hour'
        ELSE 'Unknown'
    END AS patient_onset_age_unit,

    -- Decode patient_sex in place
    CASE 
        WHEN patient_sex = 1 THEN 'Male'
        WHEN patient_sex = 2 THEN 'Female'
        WHEN patient_sex = 0 THEN 'Unknown (Missing, unspecified, or unclear data)'
        ELSE 'Unknown'
    END AS patient_sex,

    patient_death_date

FROM raw_reports