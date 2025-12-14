{{ config(materialized='table') }}

select 
    distinct customer_id, delivery_city, delivery_zip
from {{ source("silver", "orders") }}
