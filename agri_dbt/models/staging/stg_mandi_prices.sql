{{ config(materialized='view', schema='staging') }}

with source as (

    select * from {{ source('raw', 'mandi_prices') }}

),

cleaned as (

    select
        -- Identifiers
        trim(state)          as state,
        trim(district)       as district,
        trim(market)         as market,
        trim(commodity)      as commodity,
        nullif(trim(variety), '') as variety,
        nullif(trim(grade), '')   as grade,

        -- Parse DD/MM/YYYY string into a real date
        to_date(arrival_date, 'DD/MM/YYYY') as arrival_date,

        -- Prices in rupees per quintal
        min_price    as min_price_inr,
        max_price    as max_price_inr,
        modal_price  as modal_price_inr,

        -- Audit: when this row was ingested
        ingested_at,
        ingested_at::date as ingested_at_date

    from source

    where modal_price is not null
      and modal_price > 0

)

select * from cleaned