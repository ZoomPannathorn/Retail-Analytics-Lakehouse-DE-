select
    cast(order_id as integer) as order_id,
    cast(customer_id as integer) as customer_id,
    cast(product_id as integer) as product_id,
    cast(quantity as integer) as quantity,
    cast(order_timestamp as timestamp) as order_timestamp,
    cast(shipping_date as date) as shipping_date,
    order_status,
    cast(discount_pct as numeric(10, 2)) as discount_pct
from silver.order_facts

