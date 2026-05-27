WITH reports AS (
    SELECT * FROM "airflow"."public"."stg_reports"
),
drugs AS (
    SELECT * FROM "airflow"."public"."stg_drugs"
),
reactions AS (
    SELECT * FROM "airflow"."public"."stg_reactions"
)

SELECT
    r.safetyreportid,
    r.safetyreportversion,
    r.receivedate,
    r.transmissiondate,
    r.primarysourcecountry,
    r.occurcountry,
    r.reporttype,
    r.serious,
    r.seriousnessdeath,
    r.seriousnessdisabling,
    r.seriousnessother,
    r.patient_onset_age,
    r.patient_onset_age_unit,
    r.patient_sex,
    r.patient_death_date,
    d.drug_id,
    d.medicinal_product,
    d.drug_characterization,
    rx.reaction_id,
    rx.reaction_meddra_pt
FROM reports r
LEFT JOIN drugs d ON r.safetyreportid = d.safetyreportid
LEFT JOIN reactions rx ON r.safetyreportid = rx.safetyreportid