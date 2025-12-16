select
    customer_id,
    email_hash as email_id,       -- 改名，強調這是 ID 用途
    phone_number_hash as phone_id,
    real_name_masked as customer_name,
    home_address_generalized as city_state_zip, -- 只保留城市層級
    original_city,
    original_state,
from {{ ref('int_customer_security') }}