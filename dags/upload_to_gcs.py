import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# --- 環境變數設定 ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET")

current_dag_dir = os.path.dirname(os.path.abspath(__file__))

DBT_PROJECT_DIR = os.path.join(os.path.dirname(current_dag_dir), "dbt_ecommerce")
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# 定義要上傳的靜態檔案清單
STATIC_FILES = [
    "customer_pii.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]

FILE_SCHEMAS = {
    "customer_pii": [
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "real_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        {"name": "home_address", "type": "STRING", "mode": "NULLABLE"},
        {"name": "original_city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "original_state", "type": "STRING", "mode": "NULLABLE"},
    ],
    "product_category_name_translation": [
        {"name": "product_category_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_category_name_english", "type": "STRING", "mode": "NULLABLE"},
    ],
}

# 定義預設參數
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "olist_etl_pipeline",
    default_args=default_args,
    description="ETL: Local CSV -> GCS -> BigQuery -> dbt Models",
    schedule="@daily",  # 每天執行
    start_date=datetime(2025, 12, 9),
    catchup=False,  # 設為 False 避免一次跑幾百天的歷史資料 (開發階段建議)
    tags=["ecommerce", "dbt", "bigquery"],
    max_active_tasks=3,
) as dag:

    # ==========================================
    # Group 1: 每日匯率處理 (原本的邏輯)
    # ==========================================
    with TaskGroup("daily_exchange_rates") as exchange_rates_group:

        fetch_data = BashOperator(
            task_id="fetch_script",
            bash_command="""
                python /opt/airflow/scripts/fetch_exchange_rates.py \
                --start_date {{ macros.ds_add(ds, -7) }} \
                --end_date {{ ds }}
            """,
        )

        upload_rates = LocalFilesystemToGCSOperator(
            task_id="upload_gcs",
            src="/opt/airflow/data/exchange_rates.csv",  # 本地永遠是這一個檔案
            dst="raw/exchange_rates/full_history/exchange_rates.csv",  # GCS 上也固定位置
            bucket=BUCKET_NAME,
        )

        load_rates_bq = GCSToBigQueryOperator(
            task_id="load_bq",
            bucket=BUCKET_NAME,
            source_objects=["raw/exchange_rates/full_history/exchange_rates.csv"],
            destination_project_dataset_table=f"{PROJECT_ID}.olist_raw.exchange_rates",
            write_disposition="WRITE_TRUNCATE",  # <--- 關鍵修改：清空重寫
            source_format="CSV",
            skip_leading_rows=1,
            autodetect=True,
        )

        fetch_data >> upload_rates >> load_rates_bq

    # ==========================================
    # Group 2: 靜態檔案處理 (上傳 GCS + 載入 BigQuery)
    # ==========================================
    with TaskGroup("upload_static_archives") as static_group:

        for filename in STATIC_FILES:
            # 1. 處理 ID 與 Table Name
            # 去除 .csv 副檔名，並將點號轉為底線 (BigQuery Table 名稱不支援點號)
            clean_id = filename.replace(".csv", "").replace(".", "_")
            table_name = clean_id  # 直接用處理過的檔名當 Table Name

            # 2. 上傳到 GCS 的 Task
            upload_task = LocalFilesystemToGCSOperator(
                task_id=f"upload_{clean_id}",
                src=f"/opt/airflow/data/archive/{filename}",
                dst=f"raw/archive/{filename}",
                bucket=BUCKET_NAME,
                # [新增] 設定分塊大小 (例如 10MB)
                # 這可以大幅降低記憶體峰值，防止 OOM
                chunk_size=10 * 1024 * 1024,
            )

            specified_schema = FILE_SCHEMAS.get(clean_id)

            # 3. 載入到 BigQuery 的 Task
            load_task = GCSToBigQueryOperator(
                task_id=f"load_{clean_id}_to_bq",
                bucket=BUCKET_NAME,
                source_objects=[f"raw/archive/{filename}"],
                # 目標 Table：dataset 固定為 olist_raw，Table 名稱動態產生
                destination_project_dataset_table=f"{PROJECT_ID}.olist_raw.{table_name}",
                # 重要：使用 WRITE_TRUNCATE (覆蓋模式)
                # 這樣即使你重跑 DAG，也不會產生重複的訂單資料
                write_disposition="WRITE_TRUNCATE",
                source_format="CSV",
                # 如果我們有提供 Schema，就使用它；否則設為 None
                schema_fields=specified_schema,
                # 如果提供了 Schema，autodetect 最好關掉，避免衝突
                autodetect=(specified_schema is None),
                # 因為我們提供了 Schema (或希望 BQ 猜)，我們必須告訴 BQ 跳過第一行標頭
                # 否則標頭會被當成資料寫入，或者導致型別錯誤
                skip_leading_rows=1,
                allow_quoted_newlines=True,  # 允許 CSV 欄位內容中包含換行符號 (解決 Review Comment 問題)
                quote_character='"',  # 確保它是用雙引號包夾字串
            )

            # 4. 設定這兩個 Task 的順序：先上傳 -> 再載入
            upload_task >> load_task

    # ==========================================
    # 3. 執行 dbt 轉換 (dbt Transformation)
    # ==========================================
    with TaskGroup("dbt_transformation") as dbt_group:

        # 3.1 安裝依賴 (dbt deps)
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
        )

        # 3.2 dbt build 會同時執行模型並在每個步驟後立即測試
        dbt_build = BashOperator(
            task_id="dbt_build",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir {DBT_PROFILES_DIR}",
        )

        dbt_deps >> dbt_build

    # ==========================================
    # 4. 設定整體 DAG 依賴關係
    # ==========================================

    # 只有當「靜態檔案上傳」與「匯率更新」都完成後，才開始跑 dbt
    [static_group, exchange_rates_group] >> dbt_group
