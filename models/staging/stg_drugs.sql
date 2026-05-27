WITH raw_drugs AS (
    SELECT * FROM {{ source('public', 'drugs') }}
)

SELECT
    drug_id,
    safetyreportid,
    -- Strips non-alphanumeric characters from the start, then trims and capitalizes
    UPPER(TRIM(REGEXP_REPLACE(medicinal_product, '^[^a-zA-Z0-9]+', ''))) AS medicinal_product,
    
    CASE 
        WHEN drug_characterization = 1 THEN 'Suspect'
        WHEN drug_characterization = 2 THEN 'Concomitant'
        WHEN drug_characterization = 3 THEN 'Interacting'
        ELSE 'Unspecified'
    END AS drug_characterization

FROM raw_drugs
