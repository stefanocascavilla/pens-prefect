import requests
import json

from prefect import flow, task
from prefect.blocks.system import Secret


ACTIVE_CAMPAIGN_BASE_URL = 'https://miapensione.api-us1.com'


@task(
    name='extract_active_campaign_tags',
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def extract_active_campaign_tags() -> list[tuple]:
    ac_token = Secret.load("active-campaign-token")

    print('Extracting Tags from Active Campaign...')
    tags_url = f'{ACTIVE_CAMPAIGN_BASE_URL}/api/3/tags'

    tag_list = []
    offset = 0
    while True:
        print(f'Iteration: {offset / 100}')
        ac_response = requests.get(
            url=f'{tags_url}?limit=100&offset={offset}',
            headers={
                'Accept': 'application/json',
                'Api-Token': ac_token
            }
        )
        if ac_response.status_code != 200:
            raise Exception(f'Error while performing Call to AC - Tags: {ac_response.status_code}')

        ac_response = json.loads(ac_response.text)
        if len(ac_response['tags']) > 0:
            tmp_list = [
                (single_tag['id'], single_tag['tagType'], single_tag['tag'])
                for single_tag in ac_response['tags']
            ]
            tag_list.extend(tmp_list)
        else:
            break
        
        offset += 100
    
    print(f'Successfully extracted {len(tag_list)} Tags from AC')
    return tag_list



@flow(
    name='get_active_campaign_tags',
    log_prints=True,
    timeout_seconds=3600
)
def get_active_campaign_tags():
    tag_list = extract_active_campaign_tags()