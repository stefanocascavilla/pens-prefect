from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/stefanocascavilla/pens-prefect.git",
        entrypoint="flows/ETL_get_active_campaign_tags.py:get_active_campaign_tags",
    ).deploy(
        name="get_active_campaign_tags_deployment",
        work_pool_name="default-managed-pool",
        cron="0 1 * * *"
    )