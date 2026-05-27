
  create view "airflow"."public"."stg_reactions__dbt_tmp"
    
    
  as (
    WITH raw_reactions AS (
    SELECT * FROM "airflow"."public"."reactions"
)

SELECT
    reaction_id,
    safetyreportid,
    -- Trim whitespace and handle occasional missing values safely
    COALESCE(TRIM(reaction_meddra_pt), 'Unknown/Unspecified Term') AS reaction_meddra_pt

FROM raw_reactions
  );