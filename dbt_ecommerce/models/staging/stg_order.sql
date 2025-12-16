with source as (

    select * from {{ source('olist_raw', 'olist_orders_dataset') }}

),

cleaned as (

    select
        -- 1. ID 欄位
        order_id,
        customer_id,

        -- 2. 訂單狀態
        order_status,

        -- 3. 時間欄位 (全部轉型為 Timestamp)
        cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
        cast(order_approved_at as timestamp) as order_approved_at,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date

    from source

)

select * from cleaned