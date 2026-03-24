select
    o.order_id,
    o.customer_id,
    c.full_name,
    c.country,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    o.order_timestamp,
    o.shipping_date,
    pay.payment_method,
    pay.payment_status,
    pay.gross_amount,
    pay.net_amount,
    round(pay.gross_amount - pay.net_amount, 2) as discount_amount,
    o.order_status,
    case when o.order_status = 'returned' then true else false end as is_returned
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('stg_products') }} p on o.product_id = p.product_id
left join {{ ref('stg_payments') }} pay on o.order_id = pay.order_id
