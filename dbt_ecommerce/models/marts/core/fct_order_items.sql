-- 把 order 和 order_items 合併成一張 Fact Table

with items as (
    select * from {{ ref('int_order_items_enriched') }}
),

orders as (
    select * from {{ ref('stg_order') }}
),

dim_products as (
    select * from {{ ref('dim_products') }} 
)

select
    -- 1. Keys
    items.order_item_sk,
    items.order_id,
    items.product_id,
    items.seller_id,
    orders.customer_id,

    -- 2. Dimensions (方便快速篩選，不用每次都 join)
    orders.order_status,
    dim_products.category_name,

    -- 3. Timestamps
    orders.order_purchase_timestamp,
    orders.order_approved_at,
    orders.order_delivered_customer_date,

    -- 4. Metrics (經過匯率轉換的金額)
    items.price_usd as revenue_usd,
    items.freight_value_usd,
    items.price_brl as revenue_brl,
    
    -- 5. Derived Metrics
    -- 譬如：計算是否延遲出貨
    case 
        when orders.order_delivered_customer_date > orders.order_estimated_delivery_date 
        then true 
        else false 
    end as is_delayed

from items
left join orders on items.order_id = orders.order_id
left join dim_products on items.product_id = dim_products.product_id