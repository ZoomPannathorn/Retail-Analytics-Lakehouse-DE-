select
    customer_id,
    full_name,
    city,
    country,
    signup_date
from {{ ref('stg_customers') }}

