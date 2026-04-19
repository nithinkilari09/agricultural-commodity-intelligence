{{ config(materialized='table', schema='marts') }}

with prices as (

    select * from {{ ref('stg_mandi_prices') }}

),

commodity_stats as (

    select
        commodity,

        -- Counts
        count(*)                                 as record_count,
        count(distinct market)                   as distinct_markets,
        count(distinct state)                    as distinct_states,
        count(distinct arrival_date)             as distinct_dates,

        -- Date range
        min(arrival_date)                        as first_seen_date,
        max(arrival_date)                        as last_seen_date,

        -- Price bounds (in rupees per quintal)
        min(modal_price_inr)                     as min_observed_price_inr,
        max(modal_price_inr)                     as max_observed_price_inr,
        round(avg(modal_price_inr)::numeric, 2)  as avg_observed_price_inr

    from prices
    group by commodity

)

select * from commodity_stats
order by commodity