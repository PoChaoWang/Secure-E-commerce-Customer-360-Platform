with source as (

    select * from {{ source('olist_raw', 'olist_order_items_dataset') }}

),

cleaned as (

    select
        -- 1. 生成 Surrogate Key (方便做 Unique Test)
        -- 使用 order_id 和 order_item_id 組合生成唯一碼
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_sk,

        -- 2. ID 欄位保持原樣
        order_id,
        order_item_id,
        product_id,
        seller_id,

        -- 3. 時間欄位轉換
        cast(shipping_limit_date as timestamp) as shipping_limit_date,

        -- 4. 金額欄位轉換為 Numeric (財務計算用)
        cast(price as numeric) as price,
        cast(freight_value as numeric) as freight_value

    from source

)

select * from cleaned