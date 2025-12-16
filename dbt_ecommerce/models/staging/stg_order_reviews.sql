with source as (

    select * from {{ source('olist_raw', 'olist_order_reviews_dataset') }}

),

cleaned as (

    select
        -- 1. ID 欄位
        review_id,
        order_id,

        -- 2. 分數 (轉為整數)
        cast(review_score as int64) as review_score,

        -- 3. 評論標題 (處理空值)
        case 
            when review_comment_title = 'NaN' then null 
            else review_comment_title 
        end as review_comment_title,

        -- 4. 評論內容 (處理換行與空值)
        -- replace(..., '\n', ' '): 把換行變成空白
        case 
            when review_comment_message = 'NaN' then null
            else replace(review_comment_message, '\n', ' ')
        end as review_comment_message,

        -- 5. 時間欄位
        cast(review_creation_date as timestamp) as review_creation_date,
        cast(review_answer_timestamp as timestamp) as review_answer_timestamp

    from source

)

select * from cleaned