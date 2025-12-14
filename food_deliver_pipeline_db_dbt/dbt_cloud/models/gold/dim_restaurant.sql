{{ config(materialized='table') }}

with latest_updates as (
    select *
    from 
    (
        select *,
            row_number() over (partition by restaurant_id order by update_timestamp desc) rn
        from {{ source("silver", "restaurant_updates") }}
    ) 
    where rn = 1
)

select
    restaurant_id,
    restaurant_name,
    average_prep_time_minutes,
    rating,
    total_reviews,
    is_accepting_orders,
    update_type,
    update_timestamp
from latest_updates
