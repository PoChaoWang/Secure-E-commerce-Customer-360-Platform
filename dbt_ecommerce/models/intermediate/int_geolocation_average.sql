
-- 目的： stg_geolocation 表通常有問題，同一個 Zip Code 可能會有數百個不同的 Lat/Lng (因為 GPS 飄移)。如果不處理，直接 Join 會導致資料爆炸。 邏輯： 針對 Zip Code 取經緯度的平均值，確保由 Zip Code 到座標是 1對1 的關係。
with geo as (
    select * from {{ ref('stg_geolocation') }}
)

select
    geolocation_zip_code_prefix as zip_code,
    
    -- 取平均值來代表該郵遞區號的中心點
    avg(latitude) as latitude,
    avg(longitude) as longitude,
    
    -- 通常同一個 zip code 的城市名稱是一樣的，取第一筆即可
    -- 或是再做一次 mode (眾數) 計算
    max(city) as city,
    max(state) as state

from geo
group by 1