with source as (

    select * from {{ source('olist_raw', 'olist_order_payments_dataset') }}

),

cleaned as (

    select
        -- 1. 生成 Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['order_id', 'payment_sequential']) }} as payment_sk,

        -- 2. ID 與 順序
        order_id,
        payment_sequential,

        -- 3. 支付類型 (如果想要把底線拿掉，可以用 replace，這裡示範保持原樣)
        payment_type,

        -- 4. 分期數
        payment_installments,

        -- 5. 金額轉換
        cast(payment_value as numeric) as payment_value

    from source

)

select * from cleaned