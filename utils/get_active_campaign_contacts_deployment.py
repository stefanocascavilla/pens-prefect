from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/stefanocascavilla/pens-prefect.git",
        entrypoint="flows/ETL_get_active_campaign_contacts.py:get_active_campaign_contacts",
    ).deploy(
        name="get_active_campaign_contacts_deployment",
        work_pool_name="default-managed-pool",
        cron="0 1 * * *"
    )