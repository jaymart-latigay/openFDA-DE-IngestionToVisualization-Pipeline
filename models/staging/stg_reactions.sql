WITH raw_reactions AS (
    SELECT * FROM {{ source('public', 'reactions') }}
)

SELECT
    reaction_id,
    safetyreportid,
    -- Trim whitespace and handle occasional missing values safely
    COALESCE(TRIM(reaction_meddra_pt), 'Unknown/Unspecified Term') AS reaction_meddra_pt

FROM raw_reactions
