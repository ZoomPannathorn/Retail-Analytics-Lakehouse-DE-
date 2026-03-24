select
    customer_id,
    full_name,
    country,
    count(distinct order_id) as total_orders,
    round(sum(net_amount), 2) as lifetime_value,
    round(avg(net_amount), 2) as avg_order_value
from {{ ref('fct_orders') }}
where payment_status = 'paid'
group by 1, 2, 3
order by lifetime_value desc

