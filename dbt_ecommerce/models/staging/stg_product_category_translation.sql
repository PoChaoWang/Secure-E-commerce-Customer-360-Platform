with source as (

    select * from {{ source('olist_raw', 'product_category_name_translation') }}

),

cleaned as (

    select
        -- 1. 原始類別 (葡文) - 也是這張表的 PK
        product_category_name,

        -- 2. 英文翻譯
        product_category_name_english as category_name_english

    from source

)

select * from cleaned