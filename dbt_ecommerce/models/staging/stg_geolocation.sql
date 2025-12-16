with source as (

    select * from {{ source('olist_raw', 'olist_geolocation_dataset') }}

),

cleaned as (

    select
        -- 1. 欄位更名與選取
        CAST(geolocation_zip_code_prefix AS STRING) as geolocation_zip_code_prefix,

        -- 2. 經緯度確保為 Float
        cast(geolocation_lat as float64) as latitude,
        cast(geolocation_lng as float64) as longitude,

        -- 3. 城市名稱標準化 (最重要的一步！)
        -- 邏輯：
        -- a. TRIM: 去除前後空白
        -- b. LOWER: 轉小寫
        -- c. NORMALIZE + REGEXP_REPLACE: 去除重音符號 (例如 são -> sao)
        --    這是為了讓它可以跟 customer 表的 city 欄位順利 Join
        lower(
            regexp_replace(
                normalize(trim(geolocation_city), NFD), 
                r'\pM', ''
            )
        ) as city,

        -- 4. 州代碼標準化
        upper(trim(geolocation_state)) as state

    from source

)

select * from cleaned