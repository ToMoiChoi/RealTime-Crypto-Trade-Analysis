import urllib.request
import json
import os
import sys
from dotenv import load_dotenv

# Configure UTF-8 encoding for standard output
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Load .env configurations
load_dotenv()

TOKEN = os.getenv("ALERT_TELEGRAM_TOKEN", "")

if not TOKEN:
    print("[ERROR] ALERT_TELEGRAM_TOKEN khong co trong file .env. Vui long cau hinh truoc!")
    sys.exit(1)

def call_telegram_api(method, payload):
    url = f"https://api.telegram.org/bot{TOKEN}/{method}"
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            res = json.loads(response.read().decode("utf-8"))
            if res.get("ok"):
                print(f"✅ Da thiet lap {method} thanh cong!")
            else:
                print(f"❌ Loi thiet lap {method}: {res}")
    except urllib.error.HTTPError as he:
        print(f"❌ Loi goi API {method}: HTTP {he.code} ({he.reason})")
    except Exception as e:
        print(f"❌ Loi goi API {method}: {type(e).__name__} - {str(e)[:100]}")

def main():
    print("=" * 60)
    print("      CAU HINH THONG TIN BOT TELEGRAM QUA API")
    print("=" * 60)
    
    # 1. Set Name
    name_payload = {
        "name": "Binance Anomaly Alert Bot"
    }
    call_telegram_api("setMyName", name_payload)
    
    # 2. Set Short Description (About section in profile page)
    short_desc = "He thong canh bao giao dich bat thuong va ca voi thoi gian thuc tu Binance."
    short_payload = {
        "short_description": short_desc
    }
    call_telegram_api("setMyShortDescription", short_payload)
    
    # 3. Set Description (Shown before starting chat with the bot)
    desc = (
        "He thong phân tích giao dich và phát hien bat thuong thi truong Crypto thoi gian thuc.\n\n"
        "🔥 Cac canh bao ho tro:\n"
        "1. Whale Alert (Giao dich Ca Voi >= $1M USD)\n"
        "2. Wash Trade (Bot thao tung volume ao)\n"
        "3. Price Slippage (Truot gia manh kem volume lon)\n\n"
        "Du an thuoc de tai khoa luan tot nghiep."
    )
    desc_payload = {
        "description": desc
    }
    call_telegram_api("setMyDescription", desc_payload)
    
    # 4. Set Commands
    commands = [
        {"command": "start", "description": "Khoi dong bot va nhan tin nhan chao mung"},
        {"command": "help", "description": "Xem huong dan su dung va thong tin canh bao"},
        {"command": "status", "description": "Kiem tra trang thai hoat dong cua he thong pipeline"}
    ]
    commands_payload = {
        "commands": commands
    }
    call_telegram_api("setMyCommands", commands_payload)
    
    print("-" * 60)
    print("✅ Hoan tat thiet lap thong tin cho @AnomalyBinanceBot!")
    print("Doi voi Botpic (Anh dai dien) va Description Picture, ban bat buoc phai thay bang giao dien BotFather tren Telegram (chon Edit Botpic / Edit Description Picture).")

if __name__ == "__main__":
    main()
