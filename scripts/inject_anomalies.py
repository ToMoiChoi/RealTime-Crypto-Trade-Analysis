"""
scripts/inject_anomalies.py - Bơm dữ liệu bất thường (Anomalies) vào Kafka
để demo trực quan thuật toán phát hiện rủi ro thời gian thực của Spark.
"""

import json
import os
import sys
import time
import random
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment_events_v3")

def create_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        print(f"[ERROR] Không thể kết nối Kafka: {e}")
        sys.exit(1)

def send_trade(producer, symbol, price, quantity, time_ms=None, is_buyer_maker=False):
    if not time_ms:
        time_ms = int(time.time() * 1000)
    
    trade_id = int(time.time() * 100000) + random.randint(100, 999)
    payload = {
        "trade_id": trade_id,
        "crypto_symbol": symbol,
        "price": str(price),
        "quantity": str(quantity),
        "trade_time_ms": time_ms,
        "is_buyer_maker": is_buyer_maker,
        "buyer_order_id": random.randint(2000000, 2999999),
        "seller_order_id": random.randint(3000000, 3999999)
    }
    producer.send(KAFKA_TOPIC, value=payload)
    return payload

def main():
    print("=" * 60)
    print("      SCRIPT BƠM DỮ LIỆU THAO TÚNG THỊ TRƯỜNG VÀO KAFKA")
    print("=" * 60)
    print(f"Kết nối Kafka: {KAFKA_BOOTSTRAP_SERVERS} -> Topic: {KAFKA_TOPIC}")
    
    producer = create_producer()
    
    print("\nChọn loại bất thường muốn bơm:")
    print("  [1] Bơm 35 lệnh bình thường BTC (tạo nền cho Z-Score/Slippage)")
    print("  [2] Bơm Wash Trade (5 giao dịch cùng 1 giây/mili-giây)")
    print("  [3] Bơm Lệnh Khối lượng đột biến (Z-Score Outlier > 3.0)")
    print("  [4] Bơm Lệnh Trượt giá (Price Slippage > 1% + Khối lượng lớn)")
    print("  [5] Bơm tất cả đồng thời")
    
    try:
        choice = input("\nNhập lựa chọn của bạn (1-5): ").strip()
    except KeyboardInterrupt:
        print("\nĐã hủy.")
        return

    # Base price for BTC
    btc_base = 65000.0

    if choice == "1" or choice == "5":
        print("\n--> [1] Đang bơm 35 lệnh bình thường BTC...")
        for i in range(35):
            price = round(btc_base + random.uniform(-50, 50), 2)
            qty = round(random.uniform(0.01, 0.05), 4) # amount around $650 to $3250
            send_trade(producer, "BTCUSDT", price, qty)
            time.sleep(0.05)
        producer.flush()
        print("    [OK] Đã bơm xong 35 lệnh BTC làm nền.")

    if choice == "2" or choice == "5":
        print("\n--> [2] Đang bơm cụm Wash Trade (5 giao dịch BTC cùng 1 giây)...")
        now_ms = int(time.time() * 1000)
        # 5 trades at the exact same millisecond
        for i in range(5):
            p = btc_base
            q = round(random.uniform(0.1, 0.5), 2)
            payload = send_trade(producer, "BTCUSDT", p, q, time_ms=now_ms)
            print(f"    Sent TradeID: {payload['trade_id']} | Price: {payload['price']} | Qty: {payload['quantity']} | TimeMS: {payload['trade_time_ms']}")
        producer.flush()
        print("    [OK] Đã gửi wash trade cluster.")

    if choice == "3" or choice == "5":
        print("\n--> [3] Đang bơm lệnh khối lượng đột biến (Z-Score Outlier)...")
        # Ensure we have a high volume, e.g. amount_usd = 850k (under 1M to test Z-score without Whale Alert)
        large_qty = round(850000.0 / btc_base, 4)
        payload = send_trade(producer, "BTCUSDT", btc_base, large_qty)
        print(f"    Sent TradeID: {payload['trade_id']} | Price: {payload['price']} | Qty: {payload['quantity']} | Amount USD: ~${btc_base * large_qty:,.2f}")
        producer.flush()
        print("    [OK] Đã gửi Z-Score Outlier.")

    if choice == "4" or choice == "5":
        print("\n--> [4] Đang bơm lệnh gây trượt giá (Price Slippage > 1% + Volume lớn)...")
        # Deviate price by +2%
        high_price = round(btc_base * 1.025, 2)
        qty = 0.5 # amount around $32.5k (well above normal average of ~$2k)
        payload = send_trade(producer, "BTCUSDT", high_price, qty)
        print(f"    Sent TradeID: {payload['trade_id']} | Price: {payload['price']} (Dev: +2.5%) | Qty: {payload['quantity']} | Amount USD: ~${high_price * qty:,.2f}")
        producer.flush()
        print("    [OK] Đã gửi Price Slippage outlier.")

    print("\n[HOÀN TẤT] Dữ liệu đã được gửi thành công vào Kafka. Hãy kiểm tra màn hình log của Spark Processor hoặc database Postgres.")
    producer.close()

if __name__ == "__main__":
    main()
