with source as (

    -- 使用 source 函數引用，不要寫死 Dataset 名稱
    select * from {{ source('olist_raw', 'customer_pii') }}

),

renamed as (

    select
        customer_id,
        
        -- 簡單的標準化 (例如：轉小寫、去除前後空白)
        trim(real_name) as real_name,
        lower(trim(email)) as email,
        trim(phone_number) as phone_number,
        trim(home_address) as home_address,
        trim(original_city) as original_city,
        trim(original_state) as original_state

    from source

)

select * from renamed