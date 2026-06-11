import os
import psycopg2
from dotenv import load_dotenv

def check_all_unique():
    """
    Hàm kiểm tra tính duy nhất (khử trùng lặp 100%) của trường transaction_id
    trong bảng fact_binance_trades của cơ sở dữ liệu PostgreSQL.
    """
    # Load cấu hình từ file .env
    load_dotenv()
    
    PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
    PG_PORT = os.getenv("POSTGRES_PORT", "5432")
    PG_DB = os.getenv("POSTGRES_DB", "binance_dw")
    PG_USER = os.getenv("POSTGRES_USER", "binance")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "binance123")
    
    try:
        # Thiết lập kết nối
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        
        # Câu lệnh SQL truy vấn tổng số dòng và số lượng ID duy nhất
        query = """
            SELECT 
                COUNT(*) as total_rows, 
                COUNT(DISTINCT transaction_id) as unique_ids
            FROM fact_binance_trades;
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        total_rows = result[0]
        unique_ids = result[1]
        duplicates = total_rows - unique_ids
        
        print("=" * 70)
        print("  HỆ THỐNG KIỂM TRA ĐỐI CHIẾU DỮ LIỆU - PHÁT HIỆN TRÙNG LẶP (DEDUPLICATION)")
        print("=" * 70)
        print(f"  Tổng số bản ghi trong bảng (Total Rows)       : {total_rows:,}")
        print(f"  Số transaction_id duy nhất (Unique IDs)      : {unique_ids:,}")
        print(f"  Số lượng bản ghi trùng lặp phát hiện (Dupes) : {duplicates:,}")
        print("-" * 70)
        
        if duplicates == 0:
            print("  [✓] KẾT QUẢ: 100% DỮ LIỆU ĐÃ ĐƯỢC KHỬ TRÙNG LẶP THÀNH CÔNG!")
            print("  -> Tất cả các transaction_id trong cơ sở dữ liệu là duy nhất.")
        else:
            print(f"  [✗] CẢNH BÁO: PHÁT HIỆN CÓ {duplicates:,} BẢN GHI TRÙNG LẶP TRONG CSDL!")
            print("  Chi tiết các khóa bị trùng lặp:")
            
            # Truy vấn lấy ra các ID bị trùng để hiển thị mẫu
            dup_details_query = """
                SELECT transaction_id, COUNT(*) 
                FROM fact_binance_trades 
                GROUP BY transaction_id 
                HAVING COUNT(*) > 1 
                LIMIT 5;
            """
            cursor.execute(dup_details_query)
            dup_rows = cursor.fetchall()
            for row in dup_rows:
                print(f"    - Khóa trùng: {row[0]:<35} | Số lần xuất hiện trong CSDL: {row[1]} lần")
                
        print("=" * 70)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"[LỖI KẾT NỐI] Không thể kết nối tới cơ sở dữ liệu PostgreSQL: {e}")

if __name__ == "__main__":
    check_all_unique()
