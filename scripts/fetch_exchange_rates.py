import requests
import pandas as pd
import logging
import argparse
from datetime import datetime, timedelta
import time
import os

# 設定 Log
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_chunk(start_date, end_date, source_currency, target_currency):
    """
    發送單次 API 請求 (建議時間跨度不要超過 90 天，以獲取每日數據)
    """
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    params = {"from": source_currency, "to": target_currency}

    try:
        response = requests.get(url, params=params)
        if response.status_code == 404:
            logging.warning(
                f"No data found for {start_date} to {end_date} (possibly weekend/holiday)"
            )
            return None
        response.raise_for_status()

        data = response.json()
        if "rates" not in data:
            return None

        rates_list = []
        for date_str, rates in data["rates"].items():
            rates_list.append(
                {
                    "date": date_str,
                    "source_currency": source_currency,
                    "target_currency": target_currency,
                    "exchange_rate": rates[target_currency],
                }
            )
        return pd.DataFrame(rates_list)

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {start_date}..{end_date}: {e}")
        return None


def fetch_exchange_rates(
    start_date, end_date, source_currency="BRL", target_currency="USD"
):
    """
    主函數：負責將長日期範圍切成小塊 (Chunks)，分批抓取並合併，確保獲得每日數據。
    """
    logging.info(
        f"Fetching exchange rate data: {start_date} to {end_date} ({source_currency} -> {target_currency})"
    )

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    all_dfs = []
    current_start = start

    # 每次抓取的時間跨度 (天)，設定 90 天以確保 API 回傳每日數據
    CHUNK_DAYS = 90

    while current_start <= end:
        current_end = current_start + timedelta(days=CHUNK_DAYS)
        if current_end > end:
            current_end = end

        # 轉換為字串格式
        s_str = current_start.strftime("%Y-%m-%d")
        e_str = current_end.strftime("%Y-%m-%d")

        logging.info(f"Requesting chunk: {s_str} -> {e_str}")

        df_chunk = fetch_chunk(s_str, e_str, source_currency, target_currency)

        if df_chunk is not None and not df_chunk.empty:
            all_dfs.append(df_chunk)

        # 避免請求過於頻繁，稍微暫停
        time.sleep(0.5)

        # 下一輪的開始日期是這次結束日期的隔天
        current_start = current_end + timedelta(days=1)

    if not all_dfs:
        logging.warning("No data fetched for the entire range.")
        return None

    # 合併所有片段
    full_df = pd.concat(all_dfs, ignore_index=True)
    logging.info(f"Successfully fetched total {len(full_df)} exchange rate records")
    return full_df


def save_and_merge_data(new_df, file_path):
    """
    將新數據與現有 CSV 合併，去除重複日期，並儲存。
    """
    if new_df is None or new_df.empty:
        logging.warning("No new data to save.")
        return

    # 確保新數據日期格式正確
    new_df["date"] = pd.to_datetime(new_df["date"])

    if os.path.exists(file_path):
        logging.info(f"Found existing file at {file_path}, merging data...")
        try:
            existing_df = pd.read_csv(file_path)
            existing_df["date"] = pd.to_datetime(existing_df["date"])

            # 合併數據 (舊 + 新)
            combined_df = pd.concat([existing_df, new_df])

            # 根據日期去除重複項，保留最新的那一筆 ('last')
            # 這樣如果有修正數據 (例如從每週變每日)，新抓取的詳細數據會覆蓋舊的
            combined_df = combined_df.drop_duplicates(subset=["date"], keep="last")

            # 重新排序
            combined_df = combined_df.sort_values("date")

            # 儲存
            combined_df.to_csv(file_path, index=False)
            logging.info(f"Merged and saved {len(combined_df)} records to {file_path}")

        except Exception as e:
            logging.error(f"Error merging files: {e}")
    else:
        logging.info(f"File not found. Creating new file at {file_path}")
        new_df = new_df.sort_values("date")
        new_df.to_csv(file_path, index=False)
        logging.info(f"File saved to: {file_path}")


def main():
    # --- 1. 設定參數解析器 ---
    parser = argparse.ArgumentParser(
        description="Download and merge exchange rate data"
    )

    # 預設值為 None，觸發自動模式
    parser.add_argument(
        "--start_date", type=str, default=None, help="Start date YYYY-MM-DD"
    )
    parser.add_argument(
        "--end_date", type=str, default=None, help="End date YYYY-MM-DD"
    )
    parser.add_argument(
        "--filename", type=str, default="exchange_rates.csv", help="Output filename"
    )

    args = parser.parse_args()

    # --- 2. 決定日期範圍 ---
    if args.start_date and args.end_date:
        # 手動模式
        start_date = args.start_date
        end_date = args.end_date
        logging.info(f"Manual Mode: Fetching from {start_date} to {end_date}")
    else:
        # 自動模式 (過去 7 天)
        today = datetime.now().date()
        start_date_obj = today - timedelta(days=7)

        start_date = start_date_obj.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        logging.info(f"Auto Mode: Fetching past 7 days ({start_date} to {end_date})")

    # --- 3. 路徑處理 ---
    current_script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(current_script_path)
    data_dir = os.path.join(os.path.dirname(script_dir), "data")
    os.makedirs(data_dir, exist_ok=True)
    OUTPUT_FILE = os.path.join(data_dir, args.filename)

    # --- 4. 執行抓取與儲存 ---
    df = fetch_exchange_rates(start_date, end_date)

    if df is not None:
        save_and_merge_data(df, OUTPUT_FILE)
    else:
        logging.error("Fetch failed, no operations performed on file.")


if __name__ == "__main__":
    main()
