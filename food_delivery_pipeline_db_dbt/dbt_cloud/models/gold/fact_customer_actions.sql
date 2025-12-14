{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    action_type,
    action_timestamp,
    food_rating,
    delivery_rating,
    complaint_category,
    cancellation_reason
from {{ source("silver", "customer_actions") }}
