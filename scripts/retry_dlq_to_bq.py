"""
Retry DLQ to BigQuery - Fault-Tolerance & Self-Healing Pipeline Script
====================================================================
Tầng khôi phục lỗi (Fault-Tolerance Recovery Layer):
- Thực hiện cơ chế Tự chữa lành (Self-Healing) cho hệ thống phân tán.
- Quét các tệp tin nén Parquet bị gián đoạn (do mất mạng, hết hạn ngạch API...) trong thư mục cách ly lỗi BQ_DLQ_DIR.
- Tải bù tệp dữ liệu lên Google BigQuery để khôi phục tính nhất quán hoàn toàn của Data Warehouse.
- Di chuyển tệp thành công vào thư mục lưu trữ dlq_bq_success để đối soát và giải phóng bộ đệm.
"""

import os
import glob
import shutil
import logging
import json
import urllib.request
from dotenv import load_dotenv
from google.cloud import bigquery

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RetryDLQ_SelfHealing")

def send_alert(message, webhook_url=None, telegram_token=None, telegram_chat_id=None):
    """Gửi cảnh báo đến Discord hoặc Telegram sử dụng thư viện chuẩn urllib của Python."""
    if not message:
        return

    # 1. Gửi qua Discord Webhook
    if webhook_url:
        try:
            payload = {"content": f"⚠️ **[PIPELINE ALERT]** {message}"}
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                webhook_url,
                data=data,
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                response.read()
            logger.info("Alert sent to Discord successfully.")
        except Exception as e:
            logger.error(f"Failed to send alert to Discord: {e}")

    # 2. Gửi qua Telegram
    if telegram_token and telegram_chat_id:
        try:
            url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            payload = {
                "chat_id": telegram_chat_id,
                "text": f"⚠️ *[PIPELINE ALERT]*\n{message}",
                "parse_mode": "Markdown"
            }
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                response.read()
            logger.info("Alert sent to Telegram successfully.")
        except Exception as e:
            logger.error(f"Failed to send alert to Telegram: {e}")

def main():
    # 1. Setup paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
    
    # Load .env
    load_dotenv(ENV_PATH)
    
    # 2. Get BigQuery configs
    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET    = os.getenv("BQ_DATASET", "binance_dw")
    BQ_TABLE_FACT = "fact_binance_trades"
    GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    # Get Alerting configs
    DISCORD_WEBHOOK = os.getenv("ALERT_DISCORD_WEBHOOK_URL", "")
    TELEGRAM_TOKEN  = os.getenv("ALERT_TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("ALERT_TELEGRAM_CHAT_ID", "")
    
    try:
        MAX_DLQ_FILES = int(os.getenv("ALERT_MAX_DLQ_FILES", "20"))
    except ValueError:
        MAX_DLQ_FILES = 20
        
    try:
        MIN_DISK_FREE_PERCENT = float(os.getenv("ALERT_MIN_DISK_FREE_PERCENT", "10.0"))
    except ValueError:
        MIN_DISK_FREE_PERCENT = 10.0

    if not BQ_PROJECT_ID:
        logger.error("BQ_PROJECT_ID is not set in .env")
        return

    # Ensure GOOGLE_APPLICATION_CREDENTIALS is absolute
    if GOOGLE_CREDENTIALS and not os.path.isabs(GOOGLE_CREDENTIALS):
        GOOGLE_CREDENTIALS = os.path.join(PROJECT_ROOT, GOOGLE_CREDENTIALS)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS
        
    if not os.path.exists(GOOGLE_CREDENTIALS):
        logger.error(f"Credentials file not found at: {GOOGLE_CREDENTIALS}")
        return

    # 3. Setup DLQ Directories
    DLQ_DIR = os.getenv("BQ_DLQ_DIR", os.path.join(PROJECT_ROOT, "dlq_bq_failed"))
    SUCCESS_DIR = os.path.join(PROJECT_ROOT, "dlq_bq_success")
    os.makedirs(SUCCESS_DIR, exist_ok=True)

    if not os.path.exists(DLQ_DIR):
        logger.info(f"DLQ directory does not exist: {DLQ_DIR}. Nothing to retry.")
        return

    # Find all parquet files in the DLQ directory
    parquet_files = glob.glob(os.path.join(DLQ_DIR, "*.parquet"))
    
    # Ignore any residual combined temporary files
    parquet_files = [f for f in parquet_files if "temp_combined_retry" not in os.path.basename(f)]
    
    # --- HEALTH CHECKS ---
    # Check 1: Disk Free Space
    try:
        total, used, free = shutil.disk_usage(DLQ_DIR)
        free_percent = (free / total) * 100
        if free_percent < MIN_DISK_FREE_PERCENT:
            msg = f"Dung lượng đĩa trống quá thấp! Còn lại {free_percent:.2f}% ({free // (2**30)} GB / {total // (2**30)} GB)."
            send_alert(msg, DISCORD_WEBHOOK, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    except Exception as e:
        logger.error(f"Failed to check disk usage: {e}")

    # Check 2: DLQ Queue Size Threshold
    dlq_size = len(parquet_files)
    if dlq_size >= MAX_DLQ_FILES:
        msg = f"Số lượng file lô lỗi trong thư mục DLQ vượt ngưỡng an toàn! Số lượng hiện tại: {dlq_size} files (Ngưỡng: {MAX_DLQ_FILES})."
        send_alert(msg, DISCORD_WEBHOOK, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        
    if not parquet_files:
        logger.info("No failed parquet files found in DLQ directory. Everything is up to date!")
        return

    logger.info(f"Found {len(parquet_files)} failed batches in DLQ. Merging them into a single file...")

    # 4. Read and merge all Parquet files into a single DataFrame
    try:
        import pandas as pd
        dfs = []
        for file_path in parquet_files:
            try:
                df = pd.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to read file {file_path}: {e}")
        
        if not dfs:
            logger.warning("No valid data could be read from Parquet files in DLQ.")
            return
            
        combined_df = pd.concat(dfs, ignore_index=True)
        total_rows = len(combined_df)
        logger.info(f"Successfully merged {len(parquet_files)} files. Total merged rows: {total_rows:,}")
        
        # Enforce Int64 for Integer schema columns to prevent float inference
        int_cols = ["trade_id", "date_key", "time_key", "crypto_pair_key", "volume_category_key", 
                    "buyer_order_id", "seller_order_id", "wash_cluster_size"]
        for c in int_cols:
            if c in combined_df.columns:
                combined_df[c] = combined_df[c].astype("Int64")
        
        # Write to temporary combined Parquet file
        temp_combined_path = os.path.join(DLQ_DIR, "temp_combined_retry.parquet")
        combined_df.to_parquet(
            temp_combined_path,
            index=False,
            engine='pyarrow',
            coerce_timestamps='us',
            allow_truncated_timestamps=True
        )
    except Exception as e:
        logger.error(f"Failed to merge Parquet files: {e}")
        return

    # 5. Initialize BigQuery Client and upload the combined file
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.PARQUET,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    try:
        logger.info(f"Uploading combined Parquet file ({total_rows:,} rows) to BigQuery...")
        with open(temp_combined_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
            job.result()  # Wait for BigQuery API response (blocking)
            
        logger.info("[SUCCESS] Merged Parquet file uploaded successfully to BigQuery!")
        
        # 6. Move original files to SUCCESS_DIR and clean up
        for file_path in parquet_files:
            filename = os.path.basename(file_path)
            dest_path = os.path.join(SUCCESS_DIR, filename)
            try:
                shutil.move(file_path, dest_path)
            except Exception as move_err:
                logger.error(f"Failed to archive file {filename}: {move_err}")
                
        logger.info(f"Archived {len(parquet_files)} source files to {SUCCESS_DIR}")
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to upload merged file to BigQuery: {e}")
        logger.error("Original Parquet files are kept in DLQ for future retries.")
        
    finally:
        # Clean up temporary combined Parquet file
        if os.path.exists(temp_combined_path):
            os.remove(temp_combined_path)
            logger.info("Cleaned up temporary combined Parquet file.")

if __name__ == "__main__":
    main()
