{{ config(materialized='table', schema='marts') }}

with prices as (

    select * from {{ ref('stg_mandi_prices') }}

),

daily_aggregated as (

    select
        arrival_date,
        state,
        commodity,

        -- Observation counts
        count(*)                                        as market_observations,
        count(distinct market)                          as distinct_markets,
        count(distinct district)                        as distinct_districts,

        -- Price statistics
        min(modal_price_inr)                            as modal_price_min_inr,
        max(modal_price_inr)                            as modal_price_max_inr,
        round(avg(modal_price_inr)::numeric, 2)         as modal_price_avg_inr,

        round(
            percentile_cont(0.5) within group (order by modal_price_inr)::numeric,
            2
        )                                               as modal_price_median_inr,

        -- Spread = max - min. Cheap volatility signal.
        round(
            (max(modal_price_inr) - min(modal_price_inr))::numeric,
            2
        )                                               as price_spread_inr

    from prices
    group by arrival_date, state, commodity

)

select * from daily_aggregated
order by arrival_date desc, state, commodity