
-- 把「訂單時間」、「商品資訊」跟「匯率」綁在一起。這樣在 Gold 層要算「以美金計價的月營收」時就會非常快。 邏輯： Order Items + Orders (拿時間) + Exchange Rates (換算匯率)。
with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_order') }}
),

rates as (
    select * from {{ ref('stg_exchange_rates') }}
)

select
    items.order_item_sk,
    items.order_id,
    items.product_id,
    items.seller_id,
    orders.customer_id,
    
    -- 把訂單時間帶過來，這是分析的時間軸基準
    orders.order_purchase_timestamp,
    
    -- 原始金額 (BRL)
    items.price as price_brl,
    items.freight_value as freight_value_brl,

    -- 匯率轉換 (假設目標是轉換成 USD)
    -- 這裡要注意：如果找不到當天匯率，可能要補最近一筆或是顯示 null
    rates.exchange_rate,
    (items.price * rates.exchange_rate) as price_usd,
    (items.freight_value * rates.exchange_rate) as freight_value_usd

from items
left join orders 
    on items.order_id = orders.order_id
left join rates
    -- 這裡假設匯率表是 daily base，且我們用下單日來抓匯率
    on date(orders.order_purchase_timestamp) = rates.date
    and rates.target_currency = 'USD' 