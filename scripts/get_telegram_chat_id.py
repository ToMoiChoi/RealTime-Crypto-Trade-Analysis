import urllib.request
import json
import time
import os
import re
import sys
from dotenv import load_dotenv

# Configure UTF-8 encoding for standard output to support Vietnamese characters on Windows
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

def get_updates():
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    try:
        req = urllib.request.urlopen(url, timeout=10)
        res = json.loads(req.read().decode("utf-8"))
        return res.get("result", [])
    except Exception as e:
        print(f"Error fetching updates: {e}")
        return []

def update_env(chat_id):
    env_path = ".env"
    if not os.path.exists(env_path):
        print(".env file not found.")
        return
        
    with open(env_path, "r", encoding="utf-8") as f:
        content = f.read()
        
    # Replace ALERT_TELEGRAM_CHAT_ID=...
    new_content = re.sub(r"ALERT_TELEGRAM_CHAT_ID=.*", f"ALERT_TELEGRAM_CHAT_ID={chat_id}", content)
    
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"\n[OK] Da tu dong cap nhat ALERT_TELEGRAM_CHAT_ID={chat_id} vao file .env!")

def main():
    print("=" * 60)
    print("      TRUY XUAT TELEGRAM CHAT ID VA CAU HINH TU DONG")
    print("=" * 60)
    print("1. Hay mo Telegram va nhan vao duong link bot: https://t.me/AnomalyBinanceBot")
    print("2. Nhan nut 'Start' hoac gui mot tin nhan bat ky (vi du: 'hello').")
    print("3. He thong dang lang nghe tin nhan tu ban...")
    print("-" * 60)
    
    # Poll for messages
    while True:
        results = get_updates()
        if results:
            last_update = results[-1]
            message = last_update.get("message")
            if message:
                chat = message.get("chat")
                chat_id = chat.get("id")
                first_name = chat.get("first_name", "")
                username = chat.get("username", "")
                text = message.get("text", "")
                
                print(f"\n[TIN NHAN DA NHAN] tu {first_name} (@{username}): '{text}'")
                print(f"Chat ID cua ban la: {chat_id}")
                update_env(chat_id)
                break
        time.sleep(2)

if __name__ == "__main__":
    main()
