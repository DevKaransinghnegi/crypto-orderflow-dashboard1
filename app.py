import base64
import os
from datetime import datetime, timedelta
import threading
import json
import time
import requests
import websocket
import csv

# ==========================================
# CONFIGURATION & SETTINGS
# ==========================================
SYMBOL = "btcusdt"
WS_URL = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@trade"
SAVE_FOLDER = "logs"
ROTATE_MINUTES = 15
WHALE_THRESHOLD = 0.1  # Default threshold (e.g., 0.1 BTC for BTCUSDT)

# GitHub Config (Aapke diye gaye credentials)
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "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")
REPO_OWNER = os.environ.get("REPO_OWNER", "DevKaransinghnegi")
REPO_NAME = os.environ.get("REPO_NAME", "assingment")

if not os.path.exists(SAVE_FOLDER):
    os.makedirs(SAVE_FOLDER)

current_csv_filepath = None
csv_file_lock = threading.Lock()

# ==========================================
# GITHUB BACKUP FUNCTION
# ==========================================
def upload_to_github(file_path, filename):
    if not GITHUB_TOKEN:
        print("GitHub Token missing!")
        return
    try:
        with open(file_path, "rb") as f:
            content = f.read()
        encoded_content = base64.b64encode(content).decode("utf-8")
        url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{SAVE_FOLDER}/{filename}"
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "vnd.github+json",
        }
        data = {
            "message": f"Auto-upload order flow log: {filename}",
            "content": encoded_content,
        }
        response = requests.put(url, json=data, headers=headers)
        if response.status_code in [200, 201]:
            print(f"Successfully uploaded {filename} to GitHub!")
        else:
            print(f"GitHub Upload Failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"GitHub Upload Error: {e}")

# ==========================================
# WEBSOCKET WORKER (Backend Harvester)
# ==========================================
def run_websocket_worker():
    global current_csv_filepath
    
    csv_file = None
    csv_writer = None
    current_filename = None
    next_rotation_time = None

    def get_new_file_path():
        nonlocal current_filename, next_rotation_time, csv_file, csv_writer
        if csv_file:
            try:
                csv_file.close()
            except Exception:
                pass
            if current_filename:
                old_path = os.path.join(SAVE_FOLDER, current_filename)
                # Background thread mein GitHub par upload karenge taaki main loop block na ho
                threading.Thread(target=upload_to_github, args=(old_path, current_filename)).start()

        now = datetime.now()
        current_filename = f"{SYMBOL.upper()}_{now.strftime('%H-%M-%S')}.csv"
        filepath = os.path.join(SAVE_FOLDER, current_filename)
        
        with csv_file_lock:
            globals()['current_csv_filepath'] = filepath

        csv_file = open(filepath, mode="w", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        # CSV Headers
        csv_writer.writerow(["local_time", "price", "size", "side", "category", "usdt_value"])
        next_rotation_time = now + timedelta(minutes=ROTATE_MINUTES)
        print(f"\n[ACTIVE CSV FILE CREATED]: {current_filename}")

    get_new_file_path()

    def on_message(ws, message):
        nonlocal next_rotation_time
        try:
            data = json.loads(message)
            price = float(data["p"])
            size = float(data["q"])
            side = "sell" if data["m"] else "buy"
            usdt_val = price * size

            now = datetime.now()
            if now >= next_rotation_time:
                get_new_file_path()

            # Core Logic: Whale vs Retail categorization based on threshold
            category = "Whale" if size >= WHALE_THRESHOLD else "Retail"

            with csv_file_lock:
                if csv_writer:
                    csv_writer.writerow([now.strftime("%H:%M:%S"), price, size, side, category, f"{usdt_val:.2f}"])
                    csv_file.flush()
        except Exception as e:
            print("WS Error:", e)

    while True:
        try:
            ws = websocket.WebSocketApp(WS_URL, on_message=on_message)
            ws.run_forever()
        except Exception as e:
            print("Reconnecting WS...", e)
            time.sleep(3)

if __name__ == "__main__":
    print(f"Starting Backend Order Flow Harvester for {SYMBOL.upper()}...")
    run_websocket_worker()
