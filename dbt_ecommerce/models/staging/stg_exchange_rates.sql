with source as (

    select * from {{ source('olist_raw', 'exchange_rates') }}

),

cleaned as (

    select
        -- 1. 日期處理
        cast(date as date) as date,

        -- 2. 匯率數值
        cast(exchange_rate as float64) as exchange_rate,

        -- 3. 貨幣代碼
        source_currency,
        target_currency

    from source

)

select * from cleaned