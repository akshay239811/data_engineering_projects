{{ config(materialized='table') }}

select
    order_id,
    delivery_partner_id,
    delivery_partner_name,
    status,
    status_timestamp,
    latitude,
    longitude,
    estimated_delivery_minutes
from {{ source("silver", "delivery_status") }}
