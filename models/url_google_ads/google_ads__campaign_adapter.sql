{{ config(enabled=var('api_source') == 'google_ads') }}

-- union our dsa's back into the google_ads_url_ad_adapter
with unioned as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_google_ads__dsas_tmp'), ref('google_ads__url_ad_adapter')]
    ) }} 
),
fields as (

    select
        date_day,
        lower(account_name) as account_name,
        account_id,
        lower(campaign_name) as campaign_name,
        campaign_id,
        coalesce(lower(ad_group_name),'dnull' as ad_group_name,
        coalesce(cast(ad_group_id as BIGINT),0) as ad_group_id,
        ad_network_type,
        lower(base_url) as base_url,
        lower(url_host) as url_host,
        lower(url_path) as url_path,
        lower(utm_source) as utm_source,
        lower(utm_medium) as utm_medium,
        lower(utm_campaign) as utm_campaign,
        lower(utm_content) as utm_content,
        lower(utm_term) as utm_term,
        spend,
        clicks,
        impressions
        {% for metric in var('google_ads__campaign_stats_passthrough_metrics') %}
        , {{ metric }}
        {% endfor %}
    from unioned
)

select * from fields

