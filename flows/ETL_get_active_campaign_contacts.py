import requests
import json

from time import sleep

from prefect import flow, task, runtime, unmapped
from prefect.blocks.system import Secret

from prefect_sqlalchemy import DatabaseCredentials
from sqlalchemy import create_engine, text


ACTIVE_CAMPAIGN_BASE_URL = 'https://miapensione.api-us1.com'

AC_INSERT_QUERY = """
    INSERT INTO staging.stg_ac_contacts (id, first_name, last_name, create_date, update_date, source, source_campaign, source_adset, source_ads)
    VALUES (:id, :first_name, :last_name, :create_date, :update_date, :source, :source_campaign, :source_adset, :source_ads)
"""


@task(
    name='extract_active_campaign_contacts',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def extract_active_campaign_contacts() -> list[dict]:
    ac_token = Secret.load("active-campaign-token")
    current_date = runtime.flow_run.scheduled_start_time
    current_date_2days_sub = current_date.subtract(days=2)

    print(f'Extracting Contacts from Active Campaign > {current_date_2days_sub} and < {current_date}...')
    contacts_url = f'{ACTIVE_CAMPAIGN_BASE_URL}/api/3/contacts'

    contact_list = []
    offset = 0
    while True:
        print(f'Iteration: {offset / 100}')
        ac_response = requests.get(
            url=f'{contacts_url}?limit=100&offset={offset}&filters[created_before]={current_date.strftime("%Y-%m-%d")}&filters[created_after]={current_date_2days_sub.strftime("%Y-%m-%d")}',
            headers={
                'Accept': 'application/json',
                'Api-Token': ac_token.get()
            }
        )
        if ac_response.status_code != 200:
            raise Exception(f'Error while performing Call to AC - Contacts: {ac_response.status_code}')

        ac_response = json.loads(ac_response.text)
        if len(ac_response['contacts']) > 0:
            tmp_list = [
                {
                    'id': int(single_contact['id']),
                    'first_name': single_contact['firstName'],
                    'last_name': single_contact['lastName'],
                    'create_date': single_contact['cdate'],
                    'update_date': single_contact['udate']
                }
                for single_contact in ac_response['contacts']
            ]
            contact_list.extend(tmp_list)
        else:
            break
        
        offset += 100
        sleep(0.5)
    
    print(f'Successfully extracted {len(contact_list)} Contacts from AC')
    return contact_list

@task(
    name='extract_active_campaign_custom_fields',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def extract_active_campaign_custom_fields() -> dict:
    ac_token = Secret.load("active-campaign-token")
    custom_fields_url = f'{ACTIVE_CAMPAIGN_BASE_URL}/api/3/fields'

    print(f'Retrieving the custom fields from AC')
    # Retrieve custom fields
    ac_response = requests.get(
        url=custom_fields_url,
        headers={
            'Accept': 'application/json',
            'Api-Token': ac_token.get()
        }
    )
    if ac_response.status_code != 200:
        raise Exception(f'Error while performing Call to AC - Custom Fields: {ac_response.status_code}')
    ac_custom_fields = json.loads(ac_response.text)   

    cf_utm_dict = {
        single_custom_field['id']: single_custom_field['perstag']
        for single_custom_field in ac_custom_fields['fields'] if 'utm' in single_custom_field['title']
    }

    return cf_utm_dict

@task(
    name='enrich_contact',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def enrich_contact(contact_info: dict, utm_fields: dict) -> dict:
    ac_token = Secret.load("active-campaign-token")
    contact_tags_url = f'{ACTIVE_CAMPAIGN_BASE_URL}/api/3/contacts/{contact_info["id"]}/contactTags'
    contact_custom_fields_url = f'{ACTIVE_CAMPAIGN_BASE_URL}/api/3/contacts/{contact_info["id"]}?include=fieldValues'

    # Defining base fields (SQL constraints)
    contact_info['source'] = None
    contact_info['source_campaign'] = None
    contact_info['source_adset'] = None
    contact_info['source_ads'] = None

    # Getting Contact's tags
    print(f'Retrieving the tags for customer - {contact_info["id"]}')
    ac_response = requests.get(
        url=contact_tags_url,
        headers={
            'Accept': 'application/json',
            'Api-Token': ac_token.get()
        }
    )
    if ac_response.status_code != 200:
        raise Exception(f'Error while performing Call to AC - Contacts Tags: {ac_response.status_code}')
    ac_contact_tags = json.loads(ac_response.text)

    if len(ac_contact_tags['contactTags']) > 0:
        # Checking for tags to get source
        tags_list_ids = [single_tag['tag'] for single_tag in ac_contact_tags['contactTags']]
        
        # Is contact coming from Google?
        if "6" in tags_list_ids:
            contact_info['source'] = 'google'
            return contact_info
        # Is contact coming from Google?
        elif "5" in tags_list_ids:
            contact_info['source'] = 'youtube'
            return contact_info
    
    # Get Contact's custom fields (focus on UTM)
    print(f'Retrieving custom fields for contact - {contact_info["id"]}')
    ac_response = requests.get(
        url=contact_custom_fields_url,
        headers={
            'Accept': 'application/json',
            'Api-Token': ac_token.get()
        }
    )
    if ac_response.status_code != 200:
        raise Exception(f'Error while performing Call to AC - Contacts Custom Fields: {ac_response.status_code}')
    ac_contact_custom_fields = json.loads(ac_response.text)

    if len(ac_contact_custom_fields['fieldValues']) > 0:
        contact_custom_fields_utm = {
            utm_fields[single_custom_field['field']]: single_custom_field['value']
            for single_custom_field in ac_contact_custom_fields['fieldValues']
            if utm_fields.get(single_custom_field['field'])
        }

        # Set source
        if contact_custom_fields_utm.get('UTM_SOURCE') == 'facebook':
            contact_info['source'] = 'facebook'
        
        if contact_custom_fields_utm.get('UTM_CAMPAIGN'):
            contact_info['source_campaign'] = contact_custom_fields_utm['UTM_CAMPAIGN']

        if contact_custom_fields_utm.get('UTM_CONTENT'):
            contact_info['source_adset'] = contact_custom_fields_utm['UTM_CONTENT']

        if contact_custom_fields_utm.get('UTM_TERM'):
            contact_info['source_ads'] = contact_custom_fields_utm['UTM_TERM']
    
    print(contact_info)
    return contact_info

@task(
    name='write_into_staging_db_area',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def write_into_staging_db_areaa(ac_contacts_list: list[dict]) -> None:
    # Establish a connection to the DB
    print('Establish a connection to the DB')
    database_block = DatabaseCredentials.load('miapensione-db')
    db_url = database_block.get_connection_url()
    engine = create_engine(db_url)

    print('Inserting the records...')
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(
                text(AC_INSERT_QUERY),
                ac_contacts_list
            )
    
    print('Successfully written data into DB')


@flow(
    name='get_active_campaign_contacts',
    log_prints=True,
    timeout_seconds=3600
)
def get_active_campaign_contacts():
    # Retrieve raw contacts from AC
    contacts_list = extract_active_campaign_contacts()

    # Retrieve Custom Fields information
    custom_fields = extract_active_campaign_custom_fields()

    # Enrich Contacts info
    enriched_contacts_list = enrich_contact.map(
        contact_info=contacts_list,
        utm_fields=unmapped(custom_fields),
        wait_for=custom_fields
    )

    # Write AC data into DB
    write_to_db = write_into_staging_db_areaa(
        ac_contacts_list=enriched_contacts_list
    )