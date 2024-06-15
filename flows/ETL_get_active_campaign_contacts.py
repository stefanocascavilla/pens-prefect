import requests
import json

from time import sleep

from prefect import flow, task, runtime
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials

from google.cloud import bigquery
from google.oauth2.service_account import Credentials


ACTIVE_CAMPAIGN_BASE_URL = 'https://miapensione.api-us1.com'
GCP_DATASET = 'staging'
GCP_STG_TABLE = 'stg_ac_contacts_tmp'
GCP_TARGET_TABLE = 'stg_ac_contacts'

MERGE_BIGQUERY_TABLES = f"""
    MERGE `{GCP_DATASET}.{GCP_TARGET_TABLE}` T
    USING `{GCP_DATASET}.{GCP_STG_TABLE}` S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET
        T.update_date = S.update_date,
        T.source = S.source,
        T.source_campaign = S.source_campaign,
        T.source_adset = S.source_adset,
        T.source_ads = S.source_ads
    WHEN NOT MATCHED THEN
        INSERT (id, first_name, last_name, create_date, update_date, source, source_campaign, source_adset, source_ads)
        VALUES (S.id, S.first_name, S.last_name, S.create_date, S.update_date, S.source, S.source_campaign, S.source_adset, S.source_ads)
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
            url=f'{contacts_url}?limit=100&offset={offset}&filters[updated_before]={current_date.strftime("%Y-%m-%d")}T00:00:00&filters[updated_after]={current_date_2days_sub.strftime("%Y-%m-%d")}23:59:59&include=fieldValues,contactTags',
            headers={
                'Accept': 'application/json',
                'Api-Token': ac_token.get()
            }
        )
        if ac_response.status_code != 200:
            raise Exception(f'Error while performing Call to AC - Contacts: {ac_response.status_code}')

        ac_response = json.loads(ac_response.text)

        if len(ac_response['contacts']) > 0:
            for single_contact in ac_response['contacts']:
                tmp_contact_tags = [
                    single_tag['tag']
                    for single_tag in ac_response['contactTags']
                    if single_tag['contact'] == single_contact['id']
                ]
                tmp_contact_custom_fields = [
                    {'field': single_custom_field['field'], 'value': single_custom_field['value']}
                    for single_custom_field in ac_response['fieldValues']
                    if single_custom_field['contact'] == single_contact['id']
                ]

                contact_list.append({
                    'id': int(single_contact['id']),
                    'first_name': single_contact['firstName'],
                    'last_name': single_contact['lastName'],
                    'create_date': single_contact['created_utc_timestamp'],
                    'update_date': single_contact['updated_utc_timestamp'],
                    'tags': tmp_contact_tags,
                    'custom_fields': tmp_contact_custom_fields
                })
        else:
            break
        
        offset += 100
    
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
        for single_custom_field in ac_custom_fields['fields'] if 'Data nuova richiesta' in single_custom_field['title'] or 'utm' in single_custom_field['title']
    }

    return cf_utm_dict

@task(
    name='enrich_lead_contacts',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def enrich_lead_contacts(
    ac_contacts: list[dict],
    ac_custom_fields: dict
) -> list[dict]:
    yesterday = runtime.flow_run.scheduled_start_time.subtract(days=1)
    enriched_lead_contact = []

    for single_contact in ac_contacts:
        tmp_contact = single_contact

        if tmp_contact['create_date'].split(' ')[0] != yesterday.strftime('%Y-%m-%dT%H:%M:%S').split('T')[0]:
            continue

        tmp_contact['source'] = None
        tmp_contact['source_campaign'] = None
        tmp_contact['source_adset'] = None
        tmp_contact['source_ads'] = None

        if len(single_contact['tags']) > 0:
            # Is contact coming from Google?
            if "6" in single_contact['tags']:
                tmp_contact['source'] = 'google'
            # Is contact coming from Google?
            elif "5" in single_contact['tags']:
                tmp_contact['source'] = 'youtube'

        if tmp_contact['source'] is None:
            if len(single_contact['custom_fields']) > 0:
                contact_custom_fields = {
                    ac_custom_fields[single_custom_field['field']]: single_custom_field['value']
                    for single_custom_field in single_contact['custom_fields']
                    if ac_custom_fields.get(single_custom_field['field'])
                }

                # Set source
                if contact_custom_fields.get('UTM_SOURCE') in ['facebook', 'fb']:
                    tmp_contact['source'] = 'facebook'
                
                if contact_custom_fields.get('UTM_CAMPAIGN'):
                    tmp_contact['source_campaign'] = contact_custom_fields['UTM_CAMPAIGN']

                if contact_custom_fields.get('UTM_CONTENT'):
                    tmp_contact['source_adset'] = contact_custom_fields['UTM_CONTENT']

                if contact_custom_fields.get('UTM_TERM'):
                    tmp_contact['source_ads'] = contact_custom_fields['UTM_TERM']
        
        enriched_lead_contact.append(tmp_contact)
    
    return enriched_lead_contact

@task(
    name='enrich_old_contacts',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def enrich_old_contacts(
    ac_contacts: list[dict],
    ac_custom_fields: dict
) -> list[dict]:
    yesterday = runtime.flow_run.scheduled_start_time.subtract(days=1)
    enriched_old_contact = []

    for single_contact in ac_contacts:
        tmp_contact = single_contact

        tmp_contact['source'] = None
        tmp_contact['source_campaign'] = None
        tmp_contact['source_adset'] = None
        tmp_contact['source_ads'] = None

        if len(tmp_contact['custom_fields']) > 0:
            contact_custom_fields = {
                ac_custom_fields[single_custom_field['field']]: single_custom_field['value']
                for single_custom_field in single_contact['custom_fields']
                if ac_custom_fields.get(single_custom_field['field'])
            }

            if contact_custom_fields.get('DATA_NUOVA_RICHIESTA_GOOGLE') == yesterday.strftime('%Y-%m-%dT%H:%M:%S').split('T')[0]:
                tmp_contact['source'] = 'google'
                tmp_contact['update_date'] = yesterday.strftime('%Y-%m-%d %H:%M:%S')
            elif contact_custom_fields.get('DATA_NUOVA_RICHIESTA_LANDING_1') == yesterday.strftime('%Y-%m-%dT%H:%M:%S').split('T')[0]:
                tmp_contact['source'] = 'facebook'
                tmp_contact['update_date'] = yesterday.strftime('%Y-%m-%d %H:%M:%S')
            elif contact_custom_fields.get('DATA_NUOVA_RICHIESTA_LANDING_2') == yesterday.strftime('%Y-%m-%dT%H:%M:%S').split('T')[0]:
                tmp_contact['source'] = 'facebook'
                tmp_contact['update_date'] = yesterday.strftime('%Y-%m-%d %H:%M:%S')
            elif contact_custom_fields.get('DATA_NUOVA_RICHIESTA_LANDING_3') == yesterday.strftime('%Y-%m-%dT%H:%M:%S').split('T')[0]:
                tmp_contact['source'] = 'facebook'
                tmp_contact['update_date'] = yesterday.strftime('%Y-%m-%d %H:%M:%S')
            elif contact_custom_fields.get('DATA_NUOVA_RICHIESTA_LANDING_4') == yesterday.strftime('%Y-%m-%dT%H:%M:%S').split('T')[0]:
                tmp_contact['source'] = 'facebook'
                tmp_contact['update_date'] = yesterday.strftime('%Y-%m-%d %H:%M:%S')
            elif contact_custom_fields.get('DATA_NUOVA_RICHIESTA_YOUTUBE') == yesterday.strftime('%Y-%m-%dT%H:%M:%S').split('T')[0]:
                tmp_contact['source'] = 'youtube'
                tmp_contact['update_date'] = yesterday.strftime('%Y-%m-%d %H:%M:%S')

            if tmp_contact['source'] is None:
                continue

            # If source fb, then take UTM
            if contact_custom_fields.get('UTM_CAMPAIGN'):
                tmp_contact['source_campaign'] = contact_custom_fields['UTM_CAMPAIGN']

            if contact_custom_fields.get('UTM_CONTENT'):
                tmp_contact['source_adset'] = contact_custom_fields['UTM_CONTENT']

            if contact_custom_fields.get('UTM_TERM'):
                tmp_contact['source_ads'] = contact_custom_fields['UTM_TERM']
        else:
            continue
        
        enriched_old_contact.append(tmp_contact)
    
    return enriched_old_contact

@task(
    name='write_to_bigquery',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def write_to_bigquery(
    lead_contacts: list[dict],
    old_contacts: list[dict]
) -> None:
    full_contacts = lead_contacts + old_contacts

    if len(full_contacts) == 0:
        # No data to write, skip
        print('No data to write, skip')
        return

    gcp_credentials = GcpCredentials.load("miapensione-gcp")
    client = bigquery.Client(credentials=gcp_credentials)

    # Inserting into stg table
    table_ref = client.dataset(GCP_DATASET).table(GCP_STG_TABLE)
    errors = client.insert_rows_json(table_ref, full_contacts, row_ids=[None] * len(full_contacts))
    if errors:
        for error in errors:
            print(f"Error: {error}")
    else:
        print("Rows loaded to staging table successfully.")

    # Inserting into target table
    job = client.query(MERGE_BIGQUERY_TABLES)
    job.result()  # Wait for the job to complete
    print("Upsert from staging to target table completed.")

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

    # Get leads
    lead_contacts = enrich_lead_contacts(
        ac_contacts=contacts_list,
        ac_custom_fields=custom_fields
    )
    # Get re-activations
    old_contacts = []
    # enrich_old_contacts(
    #     ac_contacts=contacts_list,
    #     ac_custom_fields=custom_fields
    # )

    write_bigquery = write_to_bigquery(
        lead_contacts=lead_contacts,
        old_contacts=old_contacts
    )