select
    customer_id,
    real_name,
    email,
    phone_number,
    home_address,
    original_city,
    original_state,
from {{ ref('int_customer_security') }}