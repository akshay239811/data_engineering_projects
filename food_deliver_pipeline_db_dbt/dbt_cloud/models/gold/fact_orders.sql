{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    restaurant_id,
    city,
    cuisine_type,
    total_amount,
    subtotal,
    taxes,
    delivery_fee,
    order_timestamp,
    payment_method
from {{ source('silver', 'orders') }}
