import os
import sys
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

def set_cell_shading(cell, color_hex):
    """Set background color of a table cell."""
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement('w:shd')
    shd.set(qn('w:val'), 'clear')
    shd.set(qn('w:color'), 'auto')
    shd.set(qn('w:fill'), color_hex)
    tc_pr.append(shd)

def set_cell_margins(cell, top=100, bottom=100, left=150, right=150):
    """Set inner margins (padding) of a table cell in twentieths of a point (dxa)."""
    tc_pr = cell._tc.get_or_add_tcPr()
    tc_mar = OxmlElement('w:tcMar')
    for m, val in [('w:top', top), ('w:bottom', bottom), ('w:left', left), ('w:right', right)]:
        node = OxmlElement(m)
        node.set(qn('w:w'), str(val))
        node.set(qn('w:type'), 'dxa')
        tc_mar.append(node)
    tc_pr.append(tc_mar)

def add_code_block(doc, text):
    """Create a beautiful code block inside a shaded callout table."""
    tbl = doc.add_table(rows=1, cols=1)
    tbl.autofit = False
    tbl.columns[0].width = Inches(6.0)
    
    cell = tbl.cell(0, 0)
    set_cell_shading(cell, "F2F4F7")
    set_cell_margins(cell, top=120, bottom=120, left=200, right=200)
    
    # Border formatting: left border thick, others none
    tc_pr = cell._tc.get_or_add_tcPr()
    borders = OxmlElement('w:tcBorders')
    
    left = OxmlElement('w:left')
    left.set(qn('w:val'), 'single')
    left.set(qn('w:sz'), '24') # 3pt
    left.set(qn('w:space'), '0')
    left.set(qn('w:color'), '1A5276') # Deep Blue
    borders.append(left)
    
    for b in ['w:top', 'w:bottom', 'w:right']:
        node = OxmlElement(b)
        node.set(qn('w:val'), 'none')
        borders.append(node)
    tc_pr.append(borders)
    
    # Write code
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(2)
    p.paragraph_format.space_after = Pt(2)
    p.paragraph_format.line_spacing = 1.1
    
    run = p.add_run(text)
    run.font.name = 'Courier New'
    run.font.size = Pt(10.5)
    run.font.color.rgb = RGBColor(44, 62, 80)

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')

    print("[WORD GEN] Đang khởi tạo tài liệu Word...")
    doc = Document()
    
    # Page setup
    section = doc.sections[0]
    section.page_width = Inches(8.27)  # A4
    section.page_height = Inches(11.69)
    section.top_margin = Inches(1.0)
    section.bottom_margin = Inches(1.0)
    section.left_margin = Inches(1.2)  # Vietnamese standard margins: Left 3cm, right 2cm, top 2cm, bottom 2cm
    section.right_margin = Inches(0.8)
    
    # Style presets
    # Title
    title_p = doc.add_paragraph()
    title_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title_p.paragraph_format.space_before = Pt(12)
    title_p.paragraph_format.space_after = Pt(6)
    r = title_p.add_run("KỊCH BẢN DEMO THUYẾT TRÌNH KHÓA LUẬN TỐT NGHIỆP")
    r.font.name = 'Times New Roman'
    r.font.size = Pt(16)
    r.font.bold = True
    r.font.color.rgb = RGBColor(26, 82, 118) # Deep Blue
    
    # Subtitle
    sub_p = doc.add_paragraph()
    sub_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sub_p.paragraph_format.space_after = Pt(18)
    r = sub_p.add_run("Đề tài: Xây dựng Pipeline xử lý dữ liệu luồng thời gian thực từ Binance & Phát hiện hành vi bất thường (Anomaly Detection)")
    r.font.name = 'Times New Roman'
    r.font.size = Pt(12)
    r.font.italic = True
    r.font.color.rgb = RGBColor(120, 120, 120)
    
    def add_h1(text):
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(18)
        p.paragraph_format.space_after = Pt(6)
        p.paragraph_format.keep_with_next = True
        run = p.add_run(text)
        run.font.name = 'Times New Roman'
        run.font.size = Pt(13) # Heading 1 -> 13 or 14 bold
        run.font.bold = True
        run.font.color.rgb = RGBColor(26, 82, 118)
        
    def add_h2(text):
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(12)
        p.paragraph_format.space_after = Pt(4)
        p.paragraph_format.keep_with_next = True
        run = p.add_run(text)
        run.font.name = 'Times New Roman'
        run.font.size = Pt(12)
        run.font.bold = True
        run.font.color.rgb = RGBColor(44, 62, 80)
        
    def add_body(text, bold_prefix=None, italic=False):
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        p.paragraph_format.space_after = Pt(6)
        p.paragraph_format.line_spacing = 1.3 # 1.3 - 1.5 standard
        
        if bold_prefix:
            run_prefix = p.add_run(bold_prefix)
            run_prefix.font.name = 'Times New Roman'
            run_prefix.font.size = Pt(12)
            run_prefix.font.bold = True
            
        run = p.add_run(text)
        run.font.name = 'Times New Roman'
        run.font.size = Pt(12)
        run.font.italic = italic
        return p
        
    def add_bullet(text, bold_prefix=None):
        p = doc.add_paragraph(style='List Bullet')
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        p.paragraph_format.space_after = Pt(4)
        p.paragraph_format.line_spacing = 1.3
        
        if bold_prefix:
            run_prefix = p.add_run(bold_prefix)
            run_prefix.font.name = 'Times New Roman'
            run_prefix.font.size = Pt(12)
            run_prefix.font.bold = True
            
        run = p.add_run(text)
        run.font.name = 'Times New Roman'
        run.font.size = Pt(12)
        return p

    def add_quote(text):
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        p.paragraph_format.left_indent = Inches(0.4)
        p.paragraph_format.right_indent = Inches(0.4)
        p.paragraph_format.space_before = Pt(6)
        p.paragraph_format.space_after = Pt(6)
        p.paragraph_format.line_spacing = 1.3
        
        run = p.add_run(text)
        run.font.name = 'Times New Roman'
        run.font.size = Pt(11)
        run.font.italic = True
        run.font.color.rgb = RGBColor(80, 80, 80)
        return p

    # --- Write Content ---
    add_body("Tài liệu này phân chia quá trình demo thành hai luồng chính trực quan, giúp Hội đồng thấy rõ năng lực của hệ thống (Tính thời gian thực, Thiết kế Star Schema, Thuật toán phát hiện bất thường, Khả năng chịu lỗi và Báo cáo trực quan).")
    
    add_h1("I. CHUẨN BỊ TRƯỚC BUỔI THUYẾT TRÌNH")
    add_bullet(" Mở sẵn các công cụ cần thiết bao gồm: 3 cửa sổ Command Prompt/PowerShell (để chạy lệnh), PGAdmin 4 hoặc DBeaver để truy vấn CSDL PostgreSQL local, trình duyệt Console Google BigQuery (nếu có sử dụng), và báo cáo Power BI Dashboard.")
    add_bullet(" Khởi động hệ thống hạ tầng (Kafka, Zookeeper, PostgreSQL) bằng lệnh:")
    add_code_block(doc, "make start-kafka")
    add_bullet(" Khởi tạo cấu trúc cơ sở dữ liệu và nạp dữ liệu danh mục ban đầu (dim tables):")
    add_code_block(doc, "make setup-pg && make seed-pg")
    
    add_h1("II. LUỒNG DEMO 1: VẬN HÀNH DÒNG CHẢY DỮ LIỆU THỜI GIAN THỰC")
    add_body("Mục tiêu: Chứng minh dữ liệu đi từ sàn giao dịch Binance -> Ingestion (Kafka) -> Processing (Spark Stream) -> Database (PostgreSQL) theo thời gian thực với độ trễ cực thấp (< 500ms).")
    
    add_h2("Bước 1: Kích hoạt Ingestion Layer (Producer)")
    add_body("Tại cửa sổ Terminal thứ nhất, thực thi lệnh chạy live producer:", "Hành động: ")
    add_code_block(doc, "make run-live")
    add_body("Hệ thống kết nối trực tiếp đến cổng Public WebSocket của sàn Binance, liên tục lắng nghe biến động giao dịch thời gian thực của 5 cặp tiền lớn: BTC, ETH, SOL, BNB, XRP. Lớp Ingestion Layer tuân thủ nguyên lý Đơn nhiệm (Single Responsibility Principle) - chỉ nhận dữ liệu, định cấu hình nén LZ4 và đẩy ngay vào Kafka Topic payment_events_v3 để tối ưu hóa thông lượng và giảm tải cho CPU.", "Giải thích với Hội đồng: ", italic=True)
    add_body("Màn hình terminal in ra danh sách các giao dịch khớp lệnh thực tế từ Binance liên tục, hiển thị các thông tin gồm ký hiệu cặp tiền, giá khớp, khối lượng giao dịch và Trade ID.", "Hình ảnh hiển thị: ")
    
    add_h2("Bước 2: Kích hoạt Processing Layer (Spark Structured Streaming)")
    add_body("Tại cửa sổ Terminal thứ hai, thực thi lệnh chạy Spark Engine:", "Hành động: ")
    add_code_block(doc, "make run-spark")
    add_body("Spark Structured Streaming đóng vai trò là bộ não của hệ thống. Spark sẽ liên tục đọc luồng dữ liệu từ Kafka theo chu kỳ Trigger là 200ms (Micro-batch), thực hiện tuần tự 7 bước xử lý dữ liệu bao gồm: Ép kiểu dữ liệu (Type Casting), Làm sạch (Cleansing), Khử trùng lặp đa tầng kết hợp Watermark 30 giây, Tính toán chỉ số Amount_USD, Phân hạng volume, Phát hiện bất thường động, và cuối cùng là Ánh xạ khóa thay thế (Surrogate Keys) để đưa vào mô hình Star Schema.", "Giải thích với Hội đồng: ", italic=True)
    add_body("Spark bắt đầu in thông tin xử lý dữ liệu theo các Micro-batch. Mỗi micro-batch hiển thị số bản ghi xử lý và độ trễ latency tính bằng mili-giây.", "Hình ảnh hiển thị: ")
    
    add_h2("Bước 3: Kiểm chứng mô hình Star Schema trên Database (Postgres)")
    add_body("Mở DBeaver/PGAdmin và thực hiện câu lệnh truy vấn fact table:", "Hành động: ")
    add_code_block(doc, "SELECT * FROM fact_binance_trades ORDER BY trade_time DESC LIMIT 10;")
    add_body("Dữ liệu ghi nhận vào PostgreSQL được tối ưu hoàn toàn theo chuẩn mô hình Kimball Star Schema. Thay vì lưu trữ chuỗi text cồng kềnh, các trường dữ liệu đều được chuyển đổi thành Surrogate Keys dạng số nguyên (như date_key, time_key, crypto_pair_key, volume_category_key) để tối ưu hóa không gian lưu trữ và đẩy nhanh tốc độ thực hiện các truy vấn JOIN báo cáo.", "Giải thích với Hội đồng: ", italic=True)
    add_body("Thực hiện kiểm tra bảng latency để chứng minh hiệu năng ghi nhận:", "Thao tác bổ sung: ")
    add_code_block(doc, "SELECT sink_name, AVG(latency_ms) FROM fact_pipeline_latency GROUP BY sink_name;")
    add_body("Thời gian Spark xử lý và ghi xuống Postgres chỉ mất khoảng vài chục mili-giây, đảm bảo hệ thống phản hồi cực nhanh ở mức thời gian thực.", "Giải thích: ", italic=True)
    
    add_h1("III. LUỒNG DEMO 2: PHÁT HIỆN THAO TÚNG THỜI GIAN THỰC & KHẢ NĂNG CHỊU LỖI")
    add_body("Mục tiêu: Chứng minh thuật toán phát hiện bất thường hoạt động đúng như thiết kế, có khả năng phát hiện Point Anomaly (Z-Score), Collective Anomaly (Wash Trade), Contextual Anomaly (Price Slippage) thời gian thực và khả năng phục hồi dữ liệu khi sập nguồn hoặc mất kết nối.")
    
    add_h2("Bước 1: Giả lập bơm các hành vi thao túng (Anomalies)")
    add_body("Mở cửa sổ Terminal thứ ba, chạy script bơm lỗi giả lập chuyên dụng:", "Hành động: ")
    add_code_block(doc, "python scripts/inject_anomalies.py")
    add_body("Hệ thống sẽ hiển thị menu lựa chọn bơm loại lỗi tương ứng. Thực hiện lần lượt các trường hợp:")
    
    add_bullet(" Lựa chọn [1]: Bơm 35 lệnh bình thường BTC làm nền tính toán.")
    add_bullet(" Lựa chọn [2] (Wash Trade): Bơm 5 lệnh mua-bán BTC cùng giá trị tại cùng 1 mili-giây. Giải thích: Sàn CEX ẩn hoàn toàn địa chỉ ví nên ta không dùng đồ thị tìm chu trình. Hệ thống sử dụng quy tắc Collective Anomaly, đếm tần suất trùng khớp trùng giây >= 4 để gắn cờ bot wash trade.")
    add_bullet(" Lựa chọn [3] (Z-Score Outlier): Bơm 1 lệnh khối lượng lớn đột biến ($850k). Giải thích: Sử dụng quy tắc Point Anomaly. Nếu giao dịch vượt ngưỡng 3 độ lệch chuẩn (3-Sigma) so với trung bình của lô dữ liệu đang xét, nó sẽ bị gắn cờ bất thường.")
    add_bullet(" Lựa chọn [4] (Price Slippage): Bơm lệnh gây trượt giá lệch 2.5% kèm khối lượng lớn. Giải thích: Áp dụng quy tắc Contextual Anomaly - phát hiện lệnh có độ lệch giá lớn đi kèm khối lượng cao hơn mức trung bình của lô.")
    
    add_body("Truy cập cơ sở dữ liệu để chứng minh các hành vi thao túng đã được Spark gắn cờ thành công:", "Hành động kiểm chứng: ")
    add_code_block(doc, """SELECT transaction_id, price, quantity, amount_usd, z_score, price_dev_pct, wash_cluster_size, is_anomaly 
FROM fact_binance_trades 
WHERE is_anomaly = True 
ORDER BY trade_time DESC LIMIT 10;""")
    
    add_h2("Bước 2: Demo Khả năng chịu lỗi khi sập nguồn (Spark Checkpointing)")
    add_body("Mô phỏng sập nguồn bằng cách nhấn Ctrl+C tại Terminal thứ hai đang chạy Spark Processor. Lúc này, Terminal thứ nhất của Live Producer vẫn chạy liên tục để đẩy dữ liệu mới từ Binance vào Kafka. Sau đó khởi động lại Spark bằng lệnh: make run-spark.", "Hành động: ")
    add_body("Mặc dù bộ xử lý bị sập đột ngột, dữ liệu không bị mất mát mà được lưu trữ an toàn trong các phân vùng của Kafka. Khi Spark hoạt động trở lại, nó tự động đọc từ offset (dấu trang) cuối cùng lưu trong thư mục Checkpoint (/tmp/spark_checkpoint_binance_v7) để tiếp tục xử lý mượt mà. Đồng thời, cơ chế Postgres UPSERT (ON CONFLICT DO UPDATE) đảm bảo không bao giờ ghi trùng lặp dữ liệu vào kho.", "Giải thích với Hội đồng: ", italic=True)
    
    add_h2("Bước 3: Demo Cách ly dữ liệu lỗi (Dead-Letter Queue - DLQ)")
    add_body("Đổi sai thông tin cấu hình credentials Google Cloud BigQuery trong tệp .env (hoặc tắt kết nối internet) và thực hiện xả đệm Spark sang BigQuery.", "Hành động: ")
    add_body("Spark Processor sẽ thông báo upload thất bại nhưng không dừng dòng chảy dữ liệu. Hệ thống lưu trữ an toàn toàn bộ dữ liệu lỗi dưới dạng tệp nén Parquet cục bộ tại thư mục dlq_bq_failed. Sau khi khôi phục mạng hoặc sửa lại .env, thực thi script tải bù:", "Giải thích và hành động tiếp theo: ", italic=True)
    add_code_block(doc, "python scripts/retry_dlq_to_bq.py")
    add_body("Dữ liệu lỗi được nạp bù thành công lên Google BigQuery mà không bị thất thoát bất kỳ dòng dữ liệu nào.", "Kết quả: ", italic=True)
    
    add_h1("IV. TRÌNH BÀY KẾT QUẢ TRÊN POWER BI DASHBOARD")
    add_body("Mục tiêu: Đưa ra bức tranh tổng quan trực quan từ Data Warehouse phục vụ ra quyết định.")
    add_body("Mở và thuyết minh 5 màn hình Dashboard đã thiết kế (đã chụp ảnh đính kèm trong báo cáo khóa luận):")
    add_bullet(" Dashboard 1: Thanh Khoản & Dòng Tiền - Tổng quan khối lượng giao dịch thị trường, xu hướng dòng tiền tăng/giảm thời gian thực.")
    add_bullet(" Dashboard 2: Hành Vi Cá Mập - Thống kê số lượng lệnh cực lớn (>1M USD), theo dõi vị trí giao dịch của các ví Whale lớn.")
    add_bullet(" Dashboard 3: Cảnh Báo Rủi Ro - Tỷ lệ phần trăm giao dịch bất thường trên toàn thị trường, phân bổ lỗi theo các cặp coin giao dịch.")
    add_bullet(" Dashboard 4: Phát Hiện BOT Thao Túng - Biểu đồ hiển thị các cụm lệnh trùng lặp cùng giây, các hành vi tự mua tự bán tạo volume ảo.")
    add_bullet(" Dashboard 5: Hiệu Năng Pipeline - Biểu đồ giám sát latency (độ trễ) trung bình của Postgres và BigQuery để chứng minh hệ thống hoạt động ổn định.")
    
    # Save document
    filename = "kich_ban_demo.docx"
    doc.save(filename)
    print(f"[WORD GEN] Tài liệu Word đã được tạo thành công tại: {os.path.abspath(filename)}")

if __name__ == "__main__":
    main()
