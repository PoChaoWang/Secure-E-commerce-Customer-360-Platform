with source as (

    select * from {{ source('olist_raw', 'olist_products_dataset') }}

),

cleaned as (

    select
        -- 1. ID 欄位
        product_id,

        -- 2. 類別名稱 (處理空值)
        -- 如果是 null，就填補為 'unknown'，方便後續 Join 或是分類
        coalesce(product_category_name, 'unknown') as category_name,

        -- 3. 修正拼字錯誤 + 轉型為整數
        cast(product_name_lenght as int64) as name_length,
        cast(product_description_lenght as int64) as description_length,
        cast(product_photos_qty as int64) as photos_quantity,

        -- 4. 實體規格 (轉型為浮點數或整數)
        cast(product_weight_g as float64) as weight_g,
        cast(product_length_cm as float64) as length_cm,
        cast(product_height_cm as float64) as height_cm,
        cast(product_width_cm as float64) as width_cm

    from source

)

select * from cleaned