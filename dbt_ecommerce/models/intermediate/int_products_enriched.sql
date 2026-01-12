
-- 目的： stg_products 裡的類別是葡萄牙文，分析時很不方便，所以把英文翻譯 Join 進來。 邏輯： Left Join stg_products 和 stg_product_category_translation。
with products as (
    select * from {{ ref('stg_products') }}
),

translations as (
    select * from {{ ref('stg_product_category_translation') }}
)

select
    p.product_id,
    -- 優先使用英文，如果沒有英文翻譯則保留原文或顯示 Unknown
    coalesce(t.category_name_english, p.category_name) as category_name,
    p.name_length,
    p.description_length,
    p.photos_quantity,
    p.weight_g,
    p.length_cm,
    p.height_cm,
    p.width_cm

from products p
left join translations t
    on p.category_name = t.product_category_name