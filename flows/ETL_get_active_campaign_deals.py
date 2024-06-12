import requests
import json

from time import sleep
from datetime import datetime, timezone

from prefect import flow, task, runtime
from prefect.blocks.system import Secret

from prefect_sqlalchemy import DatabaseCredentials
from sqlalchemy import text


ACTIVE_CAMPAIGN_BASE_URL = 'https://miapensione.api-us1.com'

AC_INSERT_APPOINTMENT_DEALS_QUERY = """
    INSERT INTO staging.stg_ac_deals_appointment (id, contact, ts)
    VALUES (:id, :contact, :ts)
    ON CONFLICT DO NOTHING
"""
AC_INSERT_CLOSED_DEALS_QUERY = """
    INSERT INTO staging.stg_ac_deals_closed (id, contact, value, ts)
    VALUES (:id, :contact, :value, :ts)
    ON CONFLICT DO NOTHING
"""


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
            url=f'{deals_url}?limit=100&offset={offset}&filters[updated_before]={current_date_1day_add.strftime("%Y-%m-%d")}&filters[updated_after]={current_date_1day_sub.strftime("%Y-%m-%d")}&filters[stage]=105&filters[status]=0',
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
                    'ts': datetime.fromisoformat(single_deal['mdate']).astimezone(timezone.utc)
                }
                for single_deal in ac_response['deals']
            ]
            deal_list.extend(tmp_list)
        else:
            break

        offset += 100
        sleep(0.5)

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
            url=f'{deals_url}?limit=100&offset={offset}&filters[updated_before]={current_date_1day_add.strftime("%Y-%m-%d")}&filters[updated_after]={current_date_1day_sub.strftime("%Y-%m-%d")}&filters[stage]=99&filters[status]=1',
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
                    'ts': datetime.fromisoformat(single_deal['mdate']).astimezone(timezone.utc)
                }
                for single_deal in ac_response['deals'] if single_deal['edate'] is None
            ]
            deal_list.extend(tmp_list)
        else:
            break

        offset += 100
        sleep(0.5)

    print(f'Successfully extracted {len(deal_list)} Closed Deals from AC')
    return deal_list

@task(
    name='write_into_staging_db_area_deals',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def write_into_staging_db_area_deals(
    ac_deals_list: list[dict],
    query: str
) -> None:
    if len(ac_deals_list) == 0:
        # No data to write, skip
        return

    # Establish a connection to the DB
    print('Establish a connection to the DB')
    database_block = DatabaseCredentials.load('miapensione-db')
    engine = database_block.get_engine()

    print('Inserting the records deals...')
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(
                text(query),
                ac_deals_list
            )
    
    print('Successfully written data into DB')


@flow(
    name='get_active_campaign_deals',
    log_prints=True,
    timeout_seconds=3600
)
def get_active_campaign_deals():
    # Retrieve raw Appointment Deals from AC and write to DB
    appointment_deals = extract_active_campaign_deals_appointment()
    write_appointment_deals = write_into_staging_db_area_deals(
        ac_deals_list=appointment_deals,
        query=AC_INSERT_APPOINTMENT_DEALS_QUERY
    )

    # Retrieve raw Appointment Closed from AC
    closed_deals = extract_active_campaign_deals_closed()
    write_closed_deals = write_into_staging_db_area_deals(
        ac_deals_list=closed_deals,
        query=AC_INSERT_CLOSED_DEALS_QUERY
    )