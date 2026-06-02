# KỊCH BẢN DEMO THUYẾT TRÌNH KHÓA LUẬN TỐT NGHIỆP
## Đề tài: Xây dựng Pipeline xử lý dữ liệu luồng thời gian thực từ Binance & Phát hiện hành vi bất thường (Anomaly Detection)
---

Bản kịch bản này phân chia quá trình demo thành **2 luồng chính** trực quan, giúp Hội đồng thấy rõ năng lực của hệ thống (Tính thời gian thực, Thiết kế Star Schema, Thuật toán phát hiện bất thường, Khả năng chịu lỗi và Báo cáo trực quan).

---

## 🛠️ CHUẨN BỊ TRƯỚC BUỔI THUYẾT TRÌNH

1. **Mở sẵn các công cụ:**
   - 2 cửa sổ Command Prompt / PowerShell (để chạy Producer và Spark).
   - PGAdmin 4 hoặc DBeaver (kết nối tới Database PostgreSQL local).
   - Trình duyệt web mở Google BigQuery Console (nếu có dùng).
   - Bản báo cáo Power BI Dashboard liên kết với BigQuery / Postgres.
2. **Khởi động hạ tầng:**
   - Chạy lệnh docker để khởi động các container (Kafka, Zookeeper, Postgres):
     ```bash
     make start-kafka
     ```
   - Tạo cấu trúc bảng và nạp dữ liệu danh mục (Dimension tables):
     ```bash
     make setup-pg && make seed-pg
     ```

---

## 🌊 LUỒNG DEMO 1: VẬN HÀNH DÒNG CHẢY DỮ LIỆU THỜI GIAN THỰC (REAL-TIME DATA FLOW)

*Mục tiêu: Chứng minh dữ liệu đi từ sàn Binance -> Producer (Kafka) -> Spark Stream -> Database PostgreSQL theo thời gian thực (độ trễ cực thấp < 500ms).*

### Bước 1: Kích hoạt Ingestion Layer (Producer)
- **Hành động**: Tại **Terminal 1**, chạy lệnh:
  ```bash
  make run-live
  ```
- **Giải thích với Hội đồng**: 
  > *"Hệ thống kết nối trực tiếp đến cổng Public WebSocket của sàn Binance, liên tục lắng nghe biến động giao dịch thời gian thực của 5 cặp tiền lớn: BTC, ETH, SOL, BNB, XRP. Lớp Ingestion Layer tuân thủ nguyên lý Đơn nhiệm (Single Responsibility Principle) - chỉ nhận dữ liệu, định cấu hình nén LZ4 và đẩy ngay vào Kafka Topic `payment_events_v3` để tối ưu hóa thông lượng và giảm tải cho CPU."*
- **Hình ảnh hiển thị**: Màn hình Terminal 1 liên tục in ra các dòng giao dịch nhảy số liên tục từ Binance.

### Bước 2: Kích hoạt Processing Layer (Spark Structured Streaming)
- **Hành động**: Tại **Terminal 2**, chạy lệnh:
  ```bash
  make run-spark
  ```
- **Giải thích với Hội đồng**:
  > *"Spark Structured Streaming đóng vai trò là bộ não của hệ thống. Spark sẽ liên tục đọc luồng dữ liệu từ Kafka theo chu kỳ Trigger là 200ms (Micro-batch), thực hiện tuần tự 7 bước xử lý dữ liệu bao gồm: Ép kiểu dữ liệu (Type Casting), Làm sạch (Cleansing), Khử trùng lặp đa tầng kết hợp Watermark 30 giây, Tính toán chỉ số Amount_USD, Phân hạng volume, Phát hiện bất thường động, và cuối cùng là Ánh xạ khóa thay thế (Surrogate Keys) để đưa vào mô hình Star Schema."*
- **Hình ảnh hiển thị**: Terminal 2 bắt đầu in các dòng thông báo xử lý Batch (Batch 0, Batch 1, Batch 2...) đi kèm thông số latency.

### Bước 3: Kiểm chứng mô hình Star Schema trên Database (Postgres)
- **Hành động**: Mở công cụ quản lý DB (DBeaver/PGAdmin) và chạy câu lệnh kiểm tra:
  ```sql
  SELECT * FROM fact_binance_trades ORDER BY trade_time DESC LIMIT 10;
  ```
- **Giải thích với Hội đồng**:
  > *"Dữ liệu ghi nhận vào PostgreSQL được tối ưu hoàn toàn theo chuẩn mô hình Kimball Star Schema. Thay vì lưu trữ chuỗi text cồng kềnh, các trường dữ liệu đều được chuyển đổi thành Surrogate Keys dạng số nguyên (như `date_key`, `time_key`, `crypto_pair_key`, `volume_category_key`) để tối ưu hóa không gian lưu trữ và đẩy nhanh tốc độ thực hiện các truy vấn JOIN báo cáo."*
- **Đặc biệt (Hiệu năng Latency)**: Show bảng latency bằng câu lệnh:
  ```sql
  SELECT sink_name, AVG(latency_ms) FROM fact_pipeline_latency GROUP BY sink_name;
  ```
  > *"Thời gian Spark xử lý và ghi xuống Postgres chỉ mất khoảng vài chục mili-giây, đảm bảo hệ thống phản hồi cực nhanh ở mức thời gian thực."*

---

## ⚡ LUỒNG DEMO 2: PHÁT HIỆN THAO TÚNG THỜI GIAN THỰC & KHẢ NĂNG CHỊU LỖI (ANOMALY DETECTION & FAULT TOLERANCE)

*Mục tiêu: Chứng minh thuật toán phát hiện bất thường chạy đúng thiết kế và hệ thống tự phục hồi tốt khi xảy ra sự cố sập nguồn / mất kết nối mạng.*

### Bước 1: Thử nghiệm phát hiện 3 loại bất thường (Anomalies)
- **Hành động**: Mở thêm **Terminal 3** và chạy script bơm lỗi giả lập:
  ```bash
  python scripts/inject_anomalies.py
  ```
- **Thực hiện lần lượt các bài test**:
  - **Nhập `1`**: Bơm 35 lệnh bình thường để tạo nền thị trường cho Spark tính toán Z-Score.
  - **Nhập `2` (Wash Trade)**: Bơm 5 lệnh mua-bán BTC giống hệt nhau tại cùng 1 mili-giây.
    - *Giải thích*: Sàn CEX ẩn địa chỉ ví, nên thuật toán tìm chu trình đồ thị (Graph) của các nghiên cứu cũ không áp dụng được. Hệ thống chuyển sang thống kê tần suất cao (Collective Anomaly) - phát hiện cụm từ 4 lệnh trở lên trong cùng 1 giây.
  - **Nhập `3` (Z-Score Outlier)**: Bơm 1 lệnh mua khối lượng đột biến (ví dụ: trị giá $850k).
    - *Giải thích*: Thuật toán tính Z-Score động theo từng lô. Nếu giá trị giao dịch vượt ngưỡng 3 độ lệch chuẩn (3-Sigma) so với trung bình lô hiện tại, nó sẽ bị gắn cờ Point Anomaly.
  - **Nhập `4` (Price Slippage)**: Bơm lệnh trượt giá lệch 2.5% kèm volume lớn.
    - *Giải thích*: Đây là Contextual Anomaly - phát hiện lệnh có độ lệch giá lớn đi kèm khối lượng giao dịch cao hơn mức trung bình của lô.
- **Hình ảnh kiểm chứng**:
  - Xem màn hình log Spark (Terminal 2) nhận diện và báo: `[Batch X] Dynamic anomaly detection applied`.
  - Truy vấn Postgres kiểm tra cờ `is_anomaly = True`:
    ```sql
    SELECT transaction_id, price, quantity, amount_usd, z_score, price_dev_pct, wash_cluster_size, is_anomaly 
    FROM fact_binance_trades 
    WHERE is_anomaly = True 
    ORDER BY trade_time DESC LIMIT 10;
    ```
    *(Các cột chỉ số bất thường sẽ hiển thị chi tiết số liệu tính toán động).*

### Bước 2: Demo Khả năng tự phục hồi khi sập nguồn (Fault-Tolerance Checkpointing)
- **Hành động**:
  1. Tắt đột ngột Spark Processor ở Terminal 2 bằng cách nhấn `Ctrl + C` (giả lập sập nguồn hệ thống xử lý).
  2. Màn hình Producer (Terminal 1) vẫn đang chạy và đẩy dữ liệu liên tục vào Kafka.
     - *Giải thích*: *"Mặc dù bộ xử lý Spark bị sập, dữ liệu từ Binance vẫn được Kafka lưu trữ an toàn trong các phân vùng (Topic Partitions) nhờ cơ chế hàng đợi bất biến."*
  3. Bật lại Spark Processor ở Terminal 2 bằng lệnh: `make run-spark`.
- **Hình ảnh kiểm chứng**:
  - Spark không bị mất dữ liệu mà tiếp tục đọc từ offset (dấu trang) cuối cùng được ghi lại trong thư mục Checkpoint `/tmp/spark_checkpoint_binance_v7`.
  - Show log Postgres chứng minh không có bản ghi nào bị ghi trùng lặp nhờ cơ chế **PostgreSQL UPSERT (ON CONFLICT DO UPDATE)**.

### Bước 3: Demo Cách ly dữ liệu lỗi (Dead-Letter Queue - DLQ) khi mất mạng
- **Hành động**:
  1. Giả lập mất mạng hoặc đổi sai cấu quyền Credentials của Google BigQuery trong file `.env`.
  2. Spark Processor vẫn ghi nhận thành công vào Postgres nhưng khi flush bộ đệm sang BigQuery sẽ bị lỗi.
- **Hình ảnh kiểm chứng**:
  - Chỉ ra màn hình log lỗi BigQuery và Spark lập tức cách ly dữ liệu: `[BQ DLQ] Parquet saved to DLQ for retry...`.
  - Mở thư mục dự án chỉ ra file Parquet lỗi đã được lưu vào thư mục `dlq_bq_failed/`.
  - Sửa lại cấu hình `.env` cho đúng, chạy script retry:
    ```bash
    python scripts/retry_dlq_to_bq.py
    ```
    Hệ thống thông báo đẩy bù dữ liệu từ DLQ lên BigQuery thành công mà không thất thoát bất kỳ dòng dữ liệu nào.

---

## 📊 TRÌNH BÀY KẾT QUẢ TRÊN POWER BI DASHBOARD

*Mục tiêu: Đưa ra bức tranh tổng quan trực quan từ Data Warehouse phục vụ ra quyết định.*

Trình bày 5 màn hình Dashboard đã thiết kế (đã có trong file ảnh đính kèm dự án):
1. **Thanh Khoản & Dòng Tiền**: Cho biết tổng volume giao dịch thị trường, xu hướng dòng tiền tăng/giảm thời gian thực.
2. **Hành Vi Cá Mập**: Thống kê số lượng lệnh cực lớn (>1M USD), theo dõi ví trí giao dịch của Whale.
3. **Cảnh Báo Rủi Ro**: Tỷ lệ phần trăm giao dịch bất thường trên toàn thị trường, phân bổ lỗi theo cặp coin.
4. **Phát Hiện BOT Thao Túng**: Biểu đồ hiển thị các cụm lệnh trùng lặp cùng giây, các bot tự mua tự bán tạo volume ảo.
5. **Hiệu Năng Pipeline**: Biểu đồ giám sát latency (độ trễ) trung bình của Postgres và BigQuery để chứng minh hệ thống hoạt động ổn định.

---
*Kịch bản này giúp bạn chủ động dẫn dắt Hội đồng, giải thích cặn kẽ khía cạnh kỹ thuật và thuật toán, chắc chắn sẽ đạt điểm số rất cao.*
