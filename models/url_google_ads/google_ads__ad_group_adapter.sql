{{ config(enabled=var('api_source') == 'google_ads') }}

with stats as (

    select *
    from {{ var('ad_group_stats') }}

), accounts as (

    select *
    from {{ var('account') }}
    
), campaigns as (

    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = True
    
), ad_groups as (

    select *
    from {{ var('ad_group_history') }}
    where is_most_recent_record = True
    
), fields as (

    select
        stats.date_day,
        accounts.account_name,
        accounts.account_id,
        campaigns.campaign_name,
        campaigns.campaign_id,
        ad_groups.ad_group_name,
        ad_groups.ad_group_id,
        lower(stats.ad_network_type) as ad_network_type,
        sum(stats.spend) as spend,
        sum(stats.clicks) as clicks,
        sum(stats.impressions) as impressions

        {% for metric in var('google_ads__ad_group_stats_passthrough_metrics') %}
        , sum(stats.{{ metric }}) as {{ metric }}
        {% endfor %}

    from stats

    left join ad_groups
        on stats.id = ad_groups.ad_group_id

    left join campaigns
        on ad_groups.campaign_id = campaigns.campaign_id

    left join accounts
        on campaigns.account_id = accounts.account_id

    {{ dbt_utils.group_by(8) }}

), dsas as (
    -- get rows that exist in staged that do not exist in adapter
    SELECT
        a.*
    FROM
        fields a
        LEFT JOIN
        {{ref('google_ads__url_ad_adapter')}} b
        ON a.campaign_id = b.campaign_id
    WHERE
        b.campaign_id IS NULL
), unioned as (
    select * from fields
    union
    select * from dsas
)

select *
from unioned
