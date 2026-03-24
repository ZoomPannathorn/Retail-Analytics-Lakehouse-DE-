select
    cast(product_id as integer) as product_id,
    product_name,
    category,
    cast(unit_price as numeric(10, 2)) as unit_price
from silver.products

