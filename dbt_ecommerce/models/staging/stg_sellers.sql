with source as (

    select * from {{ source('olist_raw', 'olist_sellers_dataset') }}

),

cleaned as (

    select
        -- 1. ID
        seller_id,

        -- 2. Zip Code (轉成字串，補齊 5 位數)
        -- 巴西郵遞區號是 8 位，但這裡只給前 5 位 (Prefix)
        -- 建議統一轉為 String，避免 0 開頭的被變成整數
        cast(seller_zip_code_prefix as string) as seller_zip_code_prefix,

        -- 3. 城市名稱標準化 (去重音、轉小寫)
        -- 這是為了跟 Geolocation 表對接
        lower(
            regexp_replace(
                normalize(trim(seller_city), NFD), 
                r'\pM', ''
            )
        ) as seller_city,

        -- 4. 州代碼 (轉大寫)
        upper(trim(seller_state)) as seller_state

    from source

)

select * from cleaned