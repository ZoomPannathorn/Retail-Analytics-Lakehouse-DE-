select
    payment_id,
    cast(order_id as integer) as order_id,
    payment_method,
    cast(gross_amount as numeric(10, 2)) as gross_amount,
    cast(net_amount as numeric(10, 2)) as net_amount,
    payment_status
from raw.payments

