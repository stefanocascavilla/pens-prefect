import requests
import json

from time import sleep

from prefect import flow, task, runtime
from prefect.blocks.system import Secret


ACTIVE_CAMPAIGN_BASE_URL = 'https://miapensione.api-us1.com'


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


    print(f'{current_date} - {current_date_2days_sub}')
    print('Extracting Contacts from Active Campaign...')
    contacts_url = f'{ACTIVE_CAMPAIGN_BASE_URL}/api/3/contacts'

    contact_list = []
    offset = 0
    while True:
        print(f'Iteration: {offset / 100}')
        ac_response = requests.get(
            url=f'{contacts_url}?limit=100&offset={offset}&filters[updated_before]={current_date.strftime("%Y-%m-%d")}&filters[updated_after]={current_date_2days_sub.strftime("%Y-%m-%d")}',
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
        sleep(0.3)
    
    print(f'Successfully extracted {len(contact_list)} Contacts from AC')
    return contact_list



@flow(
    name='get_active_campaign_contacts',
    log_prints=True,
    timeout_seconds=3600
)
def get_active_campaign_contacts():
    tag_list = extract_active_campaign_contacts()