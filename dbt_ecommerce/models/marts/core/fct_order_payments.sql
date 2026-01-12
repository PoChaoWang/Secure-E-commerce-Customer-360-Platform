
-- 提供財務使用這張table進行對帳
select
    order_id,
    total_payment_value,
    max_installments,
    payment_types,
    is_credit_card_payment,
    is_voucher_payment
from {{ ref('int_order_payments_pivoted') }}