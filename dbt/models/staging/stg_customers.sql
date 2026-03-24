select
    cast(customer_id as integer) as customer_id,
    full_name,
    city,
    country,
    cast(signup_date as date) as signup_date
from silver.customers

