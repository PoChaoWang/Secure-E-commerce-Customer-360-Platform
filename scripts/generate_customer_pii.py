import pandas as pd
from faker import Faker
import os
import numpy as np

# 初始化 Faker，使用巴西葡萄牙語系
fake = Faker("pt_BR")

# --- 設定檔案路徑 (動態路徑解決方案) ---

# 1. 取得目前這個腳本 (.py) 所在的絕對路徑 (例如: /Users/.../project/script)
script_dir = os.path.dirname(os.path.abspath(__file__))

# 2. 取得專案根目錄 (往上一層，例如: /Users/.../project)
project_root = os.path.dirname(script_dir)

# 3. 組合資料檔案的絕對路徑
# 這樣無論你在哪個目錄下執行 python 指令，都能準確找到檔案
input_customers_file = os.path.join(
    project_root, "data", "archive", "olist_customers_dataset.csv"
)
input_geolocation_file = os.path.join(
    project_root, "data", "archive", "olist_geolocation_dataset.csv"
)

# 設定輸出檔案路徑 (這裡設定存回 data/archive 資料夾，與原始資料放在一起)
output_file = os.path.join(project_root, "data", "archive", "customer_pii.csv")

print(f"Script location: {script_dir}")
print(f"Reading data from: {os.path.dirname(input_customers_file)}")

# --- 檢查檔案是否存在 ---
if not os.path.exists(input_customers_file):
    print(f"Error: Customer file not found at: {input_customers_file}")
    exit(1)

if not os.path.exists(input_geolocation_file):
    # 注意：如果你的檔案名稱是 'olist_geolocation_dataset copy.csv'，請在上面的路徑設定修改檔名
    print(f"Warning: Geolocation file not found at: {input_geolocation_file}")
    print("Address generation will lack precise coordinates.")

# --- 讀取並處理資料 ---

print("Loading datasets...")

# 1. 讀取客戶資料
df_customers = pd.read_csv(input_customers_file)
print(f"Loaded {len(df_customers)} customer records")

# 2. 讀取並處理地理位置資料
geo_data_available = False
if os.path.exists(input_geolocation_file):
    df_geo = pd.read_csv(input_geolocation_file)

    # 移除重複的 zip code，保留第一筆出現的經緯度
    df_geo_unique = df_geo.drop_duplicates(subset=["geolocation_zip_code_prefix"])[
        ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]
    ]

    print(
        f"Loaded {len(df_geo)} geolocation records (reduced to {len(df_geo_unique)} unique zip codes)"
    )

    # 合併資料 (Left Join)
    df_merged = df_customers.merge(
        df_geo_unique,
        left_on="customer_zip_code_prefix",
        right_on="geolocation_zip_code_prefix",
        how="left",
    )
    geo_data_available = True
else:
    print("Proceeding without geolocation merge...")
    df_merged = df_customers

# --- 生成 PII 資料 ---

print("Generating PII data...")

pii_data = []

for index, row in df_merged.iterrows():
    # 真實地點資訊
    real_city = row["customer_city"]
    real_state = row["customer_state"]
    real_zip = row["customer_zip_code_prefix"]

    # 假的街道名稱
    fake_street_part = fake.street_address()

    # 組合地址
    full_address = f"{fake_street_part}, {real_city} - {real_state}, {real_zip}"

    # 如果有座標，加在後面
    if geo_data_available:
        lat = row.get("geolocation_lat")
        lng = row.get("geolocation_lng")

        if not pd.isna(lat) and not pd.isna(lng):
            full_address += f" (Lat: {lat:.5f}, Lng: {lng:.5f})"

    pii_data.append(
        {
            "customer_id": row["customer_id"],
            "real_name": fake.name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "home_address": full_address,
            "original_city": real_city,
            "original_state": real_state,
        }
    )

    if (index + 1) % 1000 == 0:
        print(f"Processed {index + 1} records...")

# --- 輸出結果 ---

df_pii = pd.DataFrame(pii_data)
df_pii.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"Successfully generated: {output_file}")
print(f"Total records: {len(df_pii)}")
