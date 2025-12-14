{{ config(materialized='table') }}

with orders as (
    select order_id, explode(items) as item, order_timestamp
    from {{ source("silver", "orders") }}
)

select 
    order_id,
    item.item_id,
    item.name as item_name,
    item.price,
    item.quantity,
    item.total,
    order_timestamp
from orders

