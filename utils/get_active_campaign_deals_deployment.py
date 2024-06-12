from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/stefanocascavilla/pens-prefect.git",
        entrypoint="flows/ETL_get_active_campaign_deals.py:get_active_campaign_deals",
    ).deploy(
        name="get_active_campaign_deals_deployment",
        work_pool_name="default-managed-pool",
        cron="10 * * * *"
    )