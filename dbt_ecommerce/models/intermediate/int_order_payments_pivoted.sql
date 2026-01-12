
-- 目的： 一筆訂單可能同時用「信用卡」+「Voucher」支付。如果在分析訂單時直接 Join Payment 表，會導致訂單金額重複計算 (Fan-out)。 邏輯： 按照 order_id 做 Group By，算出這筆訂單的總支付額，以及是否使用了分期。
with payments as (
    select * from {{ ref('stg_order_payments') }}
)

select
    order_id,
    
    -- 計算這筆訂單的總支付金額
    sum(payment_value) as total_payment_value,
    
    -- 計算最大分期數
    max(payment_installments) as max_installments,
    
    -- 把支付方式攤平 (例如：credit_card, voucher)
    string_agg(distinct payment_type, ', ') as payment_types,
    
    -- 做成 flag
    max(case when payment_type = 'credit_card' then 1 else 0 end) as is_credit_card_payment,
    max(case when payment_type = 'voucher' then 1 else 0 end) as is_voucher_payment

from payments
group by order_id