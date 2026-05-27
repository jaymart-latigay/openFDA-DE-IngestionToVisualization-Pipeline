import json
import logging
from datetime import datetime, timedelta
from tkinter import Variable
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator  # Added to run Docker/dbt
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,  # Ensures chronological order day-by-day
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def stream_fda_data_to_postgres(**kwargs):
    """Hits openFDA API endpoints chronologically and paginates through ALL day records."""
    
    logical_date = kwargs['ds_nodash'] 
    API_KEY = Variable.get("OPENFDA_API_KEY")
    
    # Connect to PostgreSQL outside the loop
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    db_connection = pg_hook.get_conn()
    db_cursor = db_connection.cursor()

    def clean_date_format(date_string):
        if not date_string:
            return None
        try:
            return datetime.strptime(str(date_string), '%Y%m%d').date()
        except ValueError:
            return None

    # CHUNKED INGESTION CONFIGURATION
    limit = 1000  
    skip = 0
    keep_fetching = True

    try:
        while keep_fetching:
            # We add the &skip parameter dynamically to step through the data pages for this SINGLE day
            api_url = f"https://api.fda.gov/drug/event.json?search=receivedate:{logical_date}&limit={limit}&skip={skip}&api_key={API_KEY}"
            logger.info(f"Requesting data page (Skip: {skip}) for date {logical_date}")
            
            response = requests.get(api_url)
            
            # If openFDA returns 404, we either hit the end of the data or there is no data at all
            if response.status_code == 404:
                if skip == 0:
                    logger.info(f"🎉 No adverse events recorded by the FDA on {logical_date}.")
                else:
                    logger.info(f"Reached the end of all records available for {logical_date}.")
                break
                
            if response.status_code != 200:
                raise ValueError(f"openFDA API connectivity failure. Status Code: {response.status_code}")
                
            raw_payload = response.json()
            results = raw_payload.get('results', [])
            
            if not results:
                break

            actual_record_count = len(results)
            logger.info(f"Processing chunk of {actual_record_count} records for date {logical_date}.")

            # Deep extraction and transformation loop
            for event_record in results:
                report_id = event_record.get('safetyreportid')
                if not report_id:
                    continue

                patient_block = event_record.get('patient', {})
                death_date_raw = patient_block.get('patientdeath', {}).get('patientdeathdate') if isinstance(patient_block.get('patientdeath'), dict) else None
                
                parent_report_row = (
                    report_id,
                    int(event_record.get('safetyreportversion')) if event_record.get('safetyreportversion') else None,
                    clean_date_format(event_record.get('receivedate')),
                    clean_date_format(event_record.get('transmissiondate')),
                    event_record.get('primarysourcecountry'),
                    event_record.get('occurcountry'),
                    int(event_record.get('reporttype')) if event_record.get('reporttype') else None,
                    int(event_record.get('serious')) if event_record.get('serious') else None,
                    int(event_record.get('seriousnessdeath')) if event_record.get('seriousnessdeath') else None,
                    int(event_record.get('seriousnessdisabling')) if event_record.get('seriousnessdisabling') else None,
                    int(event_record.get('seriousnessother')) if event_record.get('seriousnessother') else None,
                    float(patient_block.get('patientonsetage')) if patient_block.get('patientonsetage') else None,
                    int(patient_block.get('patientonsetageunit')) if patient_block.get('patientonsetageunit') else None,
                    int(patient_block.get('patientsex')) if patient_block.get('patientsex') else None,
                    clean_date_format(death_date_raw)
                )

                parent_sql = """
                INSERT INTO reports (
                    safetyreportid, safetyreportversion, receivedate, transmissiondate,
                    primarysourcecountry, occurcountry, reporttype, serious,
                    seriousnessdeath, seriousnessdisabling, seriousnessother,
                    patient_onset_age, patient_onset_age_unit, patient_sex, patient_death_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (safetyreportid) DO UPDATE SET
                    safetyreportversion = EXCLUDED.safetyreportversion,
                    receivedate = EXCLUDED.receivedate,
                    transmissiondate = EXCLUDED.transmissiondate;
                """
                db_cursor.execute(parent_sql, parent_report_row)

                drugs_array = patient_block.get('drug', [])
                for drug_item in drugs_array:
                    child_drug_row = (
                        report_id,
                        drug_item.get('medicinalproduct'),
                        int(drug_item.get('drugcharacterization')) if drug_item.get('drugcharacterization') else None
                    )
                    drug_sql = """
                    INSERT INTO drugs (safetyreportid, medicinal_product, drug_characterization)
                    VALUES (%s, %s, %s);
                    """
                    db_cursor.execute(drug_sql, child_drug_row)

                reactions_array = patient_block.get('reaction', [])
                for reaction_item in reactions_array:
                    child_reaction_row = (
                        report_id,
                        reaction_item.get('reactionmeddrapt')
                    )
                    reaction_sql = """
                    INSERT INTO reactions (safetyreportid, reaction_meddra_pt)
                    VALUES (%s, %s);
                    """
                    db_cursor.execute(reaction_sql, child_reaction_row)

            # Commit the current page's chunk safely to keep the database footprint clean
            db_connection.commit()
            
            # PAGINATION CHECK CONTROL
            if actual_record_count == limit:
                skip += limit  # Shifting forward by 1000 to catch the NEXT page of the same day
            else:
                keep_fetching = False  # We found less than 1000 records, meaning the day is completely dry!

        logger.info(f"Successfully finalized ingestion processing loop for date {logical_date}.")

    except Exception as pipeline_error:
        db_connection.rollback()
        logger.error(f"Error processing records on date {logical_date}: {str(pipeline_error)}")
        raise pipeline_error
    finally:
        db_cursor.close()
        db_connection.close()

with DAG(
    'openfda_adverse_events_pipeline',
    default_args=default_args,
    description='Automated pipeline streaming openFDA historical data chronologically into Postgres.',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=10,  # Limits simultaneous active worker loops to keep your machine stable
) as dag:

    # TASK 1: Extract data from API and Load into Postgres raw tables
    run_ingestion_worker = PythonOperator(
        task_id='fetch_and_stream_openfda_records',
        python_callable=stream_fda_data_to_postgres,
    )

    # TASK 2: Run dbt to Transform data and clean strings with your regex logic
    run_dbt_transformations = BashOperator(
        task_id='run_dbt_transformations',
        bash_command="""
        docker run --network=ds240-project_fda_pipeline_net \
          -v /home/jaylatigay/repos/ds240-project:/usr/app \
          -v ~/.dbt:/root/.dbt \
          -w /usr/app \
          ghcr.io/dbt-labs/dbt-postgres:1.7.latest run
        """,
    )

    # Setting up the pipeline chain reaction (Domino Effect)
    run_ingestion_worker >> run_dbt_transformations