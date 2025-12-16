with stg_customers as (
    select * from {{ ref('stg_customer_pii') }}
),

transformed as (
    select
        customer_id,
        
        -- 1. 原始資料 (Raw) - 給行銷用
        real_name,
        email,
        phone_number,
        home_address,
        original_city,
        original_state,

        -- 2. 去識別化資料 (Anonymized) - 給 ML/分析用
        
        -- Hash 處理：用於關聯但不讓分析師知道是誰 (使用 SHA256)
        to_hex(sha256(email)) as email_hash,
        to_hex(sha256(phone_number)) as phone_number_hash,

        -- Masking 處理：完全遮蔽
        '*****' as real_name_masked,

        -- 地址泛化 (Generalization)：利用 Regex 取出第一個逗號後的內容
        -- 邏輯：尋找第一個逗號，然後取其後所有字元，並去除首尾空白
        trim(regexp_extract(home_address, r',(.*)')) as home_address_generalized

        

    from stg_customers
)

select * from transformed