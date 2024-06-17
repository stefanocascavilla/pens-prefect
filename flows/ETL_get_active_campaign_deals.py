import requests
import json

from datetime import datetime, timezone

from prefect import flow, task, runtime
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials

from google.cloud import bigquery


ACTIVE_CAMPAIGN_BASE_URL = 'https://miapensione.api-us1.com'

MERGE_BIGQUERY_APPOINTMENTS = f"""
    MERGE `mart_active_campaign.ac_deals_appointment` T
    USING `staging.stg_ac_deals_appointment_tmp` S
    ON T.id = S.id
    WHEN MATCHED THEN DO NOTHING
    WHEN NOT MATCHED THEN
        INSERT (id, contact, ts)
        VALUES (S.id, S.contact, S.ts)
"""
MERGE_BIGQUERY_CLOSED = f"""
    MERGE `mart_active_campaign.ac_deals_closed` T
    USING `staging.stg_ac_deals_closed_tmp` S
    ON T.id = S.id
    WHEN MATCHED THEN DO UPDATE SET
        T.value = S.value
    WHEN NOT MATCHED THEN
        INSERT (id, contact, value, ts)
        VALUES (S.id, S.contact, S.value, S.ts)
"""
TRUNCATE_STG_TABLE_APPOINTMENTS = 'TRUNCATE TABLE staging.stg_ac_deals_appointment_tmp'
TRUNCATE_STG_TABLE_CLOSED = 'TRUNCATE TABLE staging.stg_ac_deals_closed_tmp'


@task(
    name='extract_active_campaign_deals_appointment',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def extract_active_campaign_deals_appointment() -> list[dict]:
    ac_token = Secret.load("active-campaign-token")
    current_date_1day_sub = runtime.flow_run.scheduled_start_time.subtract(days=1)
    current_date_1day_add = runtime.flow_run.scheduled_start_time.add(days=1)

    print(f'Extracting Deals Appointment from Active Campaign > {current_date_1day_sub} and < {current_date_1day_add}...')
    deals_url = f'{ACTIVE_CAMPAIGN_BASE_URL}/api/3/deals'

    deal_list = []
    offset = 0
    while True:
        print(f'Iteration: {offset / 100}')
        ac_response = requests.get(
            url=f'{deals_url}?limit=100&offset={offset}&filters[stage]=105&filters[status]=0',
            headers={
                'Accept': 'application/json',
                'Api-Token': ac_token.get()
            }
        )
        if ac_response.status_code != 200:
            raise Exception(f'Error while performing Call to AC - Deals Appointment: {ac_response.status_code}')
        
        ac_response = json.loads(ac_response.text)
        if len(ac_response['deals']) > 0:
            tmp_list = [
                {
                    'id': int(single_deal['id']),
                    'contact': int(single_deal['contact']),
                    'ts': datetime.fromisoformat(single_deal['mdate']).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                }
                for single_deal in ac_response['deals']
            ]
            deal_list.extend(tmp_list)
        else:
            break

        offset += 100

    print(f'Successfully extracted {len(deal_list)} Appointment Deals from AC')
    return deal_list

@task(
    name='extract_active_campaign_deals_closed',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def extract_active_campaign_deals_closed() -> list[dict]:
    ac_token = Secret.load("active-campaign-token")
    current_date_1day_sub = runtime.flow_run.scheduled_start_time.subtract(days=1)
    current_date_1day_add = runtime.flow_run.scheduled_start_time.add(days=1)

    print(f'Extracting Deals Closed from Active Campaign > {current_date_1day_sub} and < {current_date_1day_add}...')
    deals_url = f'{ACTIVE_CAMPAIGN_BASE_URL}/api/3/deals'

    deal_list = []
    offset = 0
    while True:
        print(f'Iteration: {offset / 100}')
        ac_response = requests.get(
            url=f'{deals_url}?limit=100&offset={offset}&filters[stage]=99&filters[status]=1',
            headers={
                'Accept': 'application/json',
                'Api-Token': ac_token.get()
            }
        )
        if ac_response.status_code != 200:
            raise Exception(f'Error while performing Call to AC - Deals Closed: {ac_response.status_code}')
        
        ac_response = json.loads(ac_response.text)
        if len(ac_response['deals']) > 0:
            tmp_list = [
                {
                    'id': int(single_deal['id']),
                    'contact': int(single_deal['contact']),
                    'value': float(single_deal['value']) / 100,
                    'ts': datetime.fromisoformat(single_deal['mdate']).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                }
                for single_deal in ac_response['deals']
                if datetime.strptime(single_deal['mdate'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d') == runtime.flow_run.scheduled_start_time.strftime('%Y-%m-%d')
            ]
            deal_list.extend(tmp_list)
        else:
            break

        offset += 100

    print(f'Successfully extracted {len(deal_list)} Closed Deals from AC')
    return deal_list

@task(
    name='write_into_staging_db_area_deals',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def write_deals_into_bigquery(
    ac_deals_list: list[dict],
    gcp_dataset: str,
    gcp_stg_table: str,
    merge_query: str
) -> None:
    if len(ac_deals_list) == 0:
        # No data to write, skip
        print('No data to write, skip')
        return

    gcp_credentials = GcpCredentials.load("miapensione-gcp")
    client = bigquery.Client(credentials=gcp_credentials.get_credentials_from_service_account())

    # Inserting into stg table
    table_ref = client.dataset(gcp_dataset).table(gcp_stg_table)
    errors = client.insert_rows_json(table_ref, ac_deals_list, row_ids=[None] * len(ac_deals_list))
    if errors:
        for error in errors:
            print(f"Error: {error}")
    else:
        print("Rows loaded to staging table successfully.")

    # Inserting into target table
    job = client.query(merge_query)
    job.result()  # Wait for the job to complete
    print("Upsert from staging to target table completed.")

@task(
    name='truncate_stg_tables',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def truncate_stg_tables():
    gcp_credentials = GcpCredentials.load("miapensione-gcp")
    client = bigquery.Client(credentials=gcp_credentials.get_credentials_from_service_account())

    job = client.query(TRUNCATE_STG_TABLE_APPOINTMENTS)
    job.result()  # Wait for the job to complete
    job = client.query(TRUNCATE_STG_TABLE_CLOSED)
    job.result()  # Wait for the job to complete

    print("Successfully truncated stg table")


@flow(
    name='get_active_campaign_deals',
    log_prints=True,
    timeout_seconds=3600
)
def get_active_campaign_deals():
    # Retrieve raw Appointment Deals from AC and write to DB
    appointment_deals = extract_active_campaign_deals_appointment()
    write_appointment_deals = write_deals_into_bigquery(
        ac_deals_list=appointment_deals,
        gcp_dataset='staging',
        gcp_stg_table='stg_ac_deals_appointment_tmp',
        merge_query=MERGE_BIGQUERY_APPOINTMENTS
    )

    # Retrieve raw Appointment Closed from AC
    closed_deals = extract_active_campaign_deals_closed()
    write_closed_deals = write_deals_into_bigquery(
        ac_deals_list=closed_deals,
        gcp_dataset='staging',
        gcp_stg_table='stg_ac_deals_closed_tmp',
        merge_query=MERGE_BIGQUERY_CLOSED
    )

    truncate = truncate_stg_tables(
        wait_for=[write_appointment_deals, write_closed_deals]
    )