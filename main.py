# main.py (النسخة النهائية الكاملة)

import os
import random
import string
import time
import logging
import threading
import json
import queue
from datetime import datetime
from clickhouse_driver import Client

# =======================
# إعدادات ثابتة
# =======================
MAIN_CLICKHOUSE_HOST = "l5bxi83or6.eu-central-1.aws.clickhouse.cloud"
MAIN_CLICKHOUSE_USER = "default"
MAIN_CLICKHOUSE_PASSWORD = "8aJlVz_A2L4On"
DATABASE_NAME = "default"

SERVERS_TABLE = "servers_for_symbols"
CLICKHOUSE_TABLES = "CLICKHOUSE_TABLES"
SERVER_ID_FILE = "server_id.txt"

HEARTBEAT_INTERVAL = 60  # الانتظار 60 ثانية بين كل دورة في اللوب الرئيسي
DB_WRITER_BATCH_SIZE = 100
DB_WRITER_BATCH_TIMEOUT = 5

# =======================
# Logging
# =======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# =======================
# Helpers
# =======================
def rand_id(n=32):
    """Generates a random ID."""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choices(chars, k=n))

def sanitize_table_name(symbol: str, task: str) -> str:
    """Creates a sanitized, valid table name from symbol and task."""
    base = "".join(c if c.isalnum() else "_" for c in symbol).upper()
    t = "".join(c if c.isalnum() else "_" for c in task).upper()
    return f"{base}_{t}"

def get_clickhouse_client(host, password, current_client=None):
    """Establishes or validates a ClickHouse client connection."""
    if current_client:
        try:
            current_client.execute("SELECT 1")
            return current_client
        except Exception:
            try:
                current_client.disconnect()
            except Exception: pass
    return Client(host=host, user=MAIN_CLICKHOUSE_USER, password=password, database=DATABASE_NAME, secure=True)

def build_symbol_search_condition(symbol: str, n: int = 50) -> str:
    """
    Creates a WHERE clause to search for a specific symbol in columns symbol1 to symbolN.
    """
    conditions = [f'"{f"symbol{i}"}" = \'{symbol}\'' for i in range(1, n + 1)]
    return " OR ".join(conditions)

# ==========================================================
# تعريفات مهام الـ WebSocket (مع فصل داخلي للأسواق)
# ==========================================================
def parse_trade_message(data, symbol):
    return (
        data.get('e', ''), datetime.fromtimestamp(data['E'] / 1000), data.get('s', symbol),
        int(data.get('t', 0)), float(data.get('p', 0.0)), float(data.get('q', 0.0)),
        int(data.get('b', 0)), int(data.get('a', 0)), datetime.fromtimestamp(data['T'] / 1000),
        1 if data.get('m', False) else 0, 1 if data.get('M', False) else 0
    )

def parse_bookticker_message(data, symbol):
    return (
        datetime.now(), int(data.get('u', 0)), data.get('s', symbol),
        float(data.get('b', 0.0)), float(data.get('B', 0.0)),
        float(data.get('a', 0.0)), float(data.get('A', 0.0))
    )

def parse_markprice_message(data, symbol):
    return (
        data.get('e', ''), datetime.fromtimestamp(data['E'] / 1000), data.get('s', symbol),
        float(data.get('p', 0.0)), float(data.get('i', 0.0)), float(data.get('P', 0.0)),
        float(data.get('r', 0.0)), datetime.fromtimestamp(data['T'] / 1000)
    )

def parse_kline_message(data, symbol):
    k = data.get('k', {})
    return (
        data.get('e', ''), datetime.fromtimestamp(data['E'] / 1000), data.get('s', symbol),
        datetime.fromtimestamp(k.get('t', 0) / 1000), datetime.fromtimestamp(k.get('T', 0) / 1000),
        k.get('i', ''), int(k.get('f', 0)), int(k.get('L', 0)), float(k.get('o', 0.0)),
        float(k.get('c', 0.0)), float(k.get('h', 0.0)), float(k.get('l', 0.0)),
        float(k.get('v', 0.0)), int(k.get('n', 0)), k.get('x', False), float(k.get('q', 0.0)),
        float(k.get('V', 0.0)), float(k.get('Q', 0.0))
    )

STREAM_BASE_URLS = {
    "spot": "wss://stream.binance.com:9443/ws",
    "futures": "wss://fstream.binance.com/ws",
}

WEBSOCKET_HANDLERS = {
    "task_trade": {
        "market_type": "spot", "stream_name": lambda symbol: f"{symbol.lower()}@trade",
        "schema": """CREATE TABLE IF NOT EXISTS {db}.{table} (e String, E DateTime, s String, t UInt64, p Float64, q Float64, b UInt64, a UInt64, T DateTime, m UInt8, M UInt8) ENGINE = MergeTree() ORDER BY T""",
        "parser": parse_trade_message
    },
    "task_bookTicker": {
        "market_type": "spot", "stream_name": lambda symbol: f"{symbol.lower()}@bookTicker",
        "schema": """CREATE TABLE IF NOT EXISTS {db}.{table} (received_time DateTime, u UInt64, s String, b Float64, B Float64, a Float64, A Float64) ENGINE = MergeTree() ORDER BY received_time""",
        "parser": parse_bookticker_message
    },
    "task_markPrice": {
        "market_type": "futures", "stream_name": lambda symbol: f"{symbol.lower()}@markPrice@1s",
        "schema": """CREATE TABLE IF NOT EXISTS {db}.{table} (e String, E DateTime, s String, p Float64, i Float64, P Float64, r Float64, T DateTime) ENGINE = MergeTree() ORDER BY E""",
        "parser": parse_markprice_message
    },
    **{f"task_kline_{interval}": {
        "market_type": "spot", "stream_name": lambda symbol, i=interval: f"{symbol.lower()}@kline_{i}",
        "schema": """CREATE TABLE IF NOT EXISTS {db}.{table} (e String, E DateTime, s String, t DateTime, T DateTime, i String, f UInt64, L UInt64, o Float64, c Float64, h Float64, l Float64, v Float64, n UInt64, x Bool, q Float64, V Float64, Q Float64) ENGINE = MergeTree() ORDER BY t""",
        "parser": parse_kline_message
    } for interval in ["1m", "3m", "15m", "1h"]}
}

# ===============================================
# Generic Websocket Worker Engine
# ===============================================
active_ws = {}
active_ws_lock = threading.Lock()

def generic_websocket_worker(main_host, main_password, sec_host, sec_password, server_id, symbol, task_col, handler_config):
    table_name = sanitize_table_name(symbol, task_col)
    log_prefix = f"[{task_col.upper()}-{server_id[:8]}]"

    main_client, sec_client = None, None
    try:
        main_client = get_clickhouse_client(main_host, main_password)
        sec_client = get_clickhouse_client(sec_host, sec_password)
    except Exception as e:
        logging.error(f"{log_prefix} Failed to create clients: {e}")
        return

    try:
        create_q = handler_config["schema"].format(db=DATABASE_NAME, table=table_name)
        sec_client.execute(create_q)
        logging.info(f"{log_prefix} Verified/created secondary table: {table_name}")
    except Exception as e:
        logging.error(f"{log_prefix} Error creating table {table_name}: {e}")
        return

    stop_flag = threading.Event()
    data_queue = queue.Queue()

    def db_writer_loop():
        nonlocal sec_client
        batch = []
        last_insert_time = time.time()
        parser = handler_config["parser"]

        while not stop_flag.is_set() or not data_queue.empty():
            try:
                message_str = data_queue.get(timeout=1)
                data = json.loads(message_str)
                row_tuple = parser(data, symbol)
                batch.append(row_tuple)
            except queue.Empty:
                pass
            except Exception as e:
                logging.error(f"{log_prefix} Error processing message from queue: {e}")

            if batch and (len(batch) >= DB_WRITER_BATCH_SIZE or (time.time() - last_insert_time > DB_WRITER_BATCH_TIMEOUT)):
                try:
                    sec_client.execute(f"INSERT INTO {DATABASE_NAME}.{table_name} VALUES", batch)
                    logging.debug(f"{log_prefix} Inserted {len(batch)} rows into {table_name}.")
                    batch = []
                    last_insert_time = time.time()
                except Exception as e:
                    logging.error(f"{log_prefix} Batch insert failed: {e}. Reconnecting...")
                    sec_client = get_clickhouse_client(sec_host, sec_password, sec_client)
                    time.sleep(5)
        logging.info(f"{log_prefix} DB writer thread for {symbol} has stopped.")

    def run_ws_loop():
        db_writer_thread = threading.Thread(target=db_writer_loop, name=f"db-{server_id[:8]}-{symbol}", daemon=True)
        db_writer_thread.start()

        market_type = handler_config["market_type"]
        base_url = STREAM_BASE_URLS[market_type]
        stream_name = handler_config["stream_name"](symbol)
        ws_url = f"{base_url}/{stream_name}"

        def check_assignment_still_valid():
            try:
                row = main_client.execute(
                    f"SELECT symbol, task_web_socket FROM {SERVERS_TABLE} WHERE server_id = %(id)s", {'id': server_id})
                return row and str(row[0][0]) == str(symbol) and str(row[0][1]) == str(task_col)
            except Exception as e:
                logging.error(f"{log_prefix} Error checking assignment: {e}")
                return False

        def on_message(ws, message): data_queue.put(message)
        def on_error(ws, error): logging.error(f"{log_prefix} WebSocket error: {error}")
        def on_close(ws, code, reason): logging.warning(f"{log_prefix} WebSocket closed: code={code}, reason={reason}")
        def on_open(ws): logging.info(f"{log_prefix} Connected to {ws_url}")
        
        while not stop_flag.is_set():
            if not check_assignment_still_valid():
                logging.info(f"{log_prefix} Assignment no longer valid. Closing WS for {symbol}")
                break
            
            try:
                ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close, on_open=on_open)
                ws.run_forever()
            except Exception as e:
                logging.error(f"{log_prefix} Exception in run_forever: {e}")

            if not check_assignment_still_valid(): break
            logging.info(f"{log_prefix} Reconnecting in 5 seconds...")
            time.sleep(5)

        stop_flag.set()
        db_writer_thread.join(timeout=10)
        try:
            sec_client.disconnect(); main_client.disconnect()
        except: pass
        with active_ws_lock: active_ws.pop(server_id, None)
        logging.info(f"{log_prefix} WS worker for {symbol} has finished.")

    t = threading.Thread(target=run_ws_loop, name=f"ws-{server_id[:8]}-{symbol}", daemon=True)
    t.start()
    return t

def start_websocket_worker(main_host, main_pass, sec_host, sec_pass, server_id, symbol, task_col):
    """Dispatcher function to start the correct websocket worker."""
    if not task_col:
        return None

    handler_config = WEBSOCKET_HANDLERS.get(task_col)
    if not handler_config:
        logging.warning(f"[Main] No handler defined for task: '{task_col}'.")
        return None

    logging.info(f"[Main] Starting worker for {symbol} with task {task_col}")
    return generic_websocket_worker(main_host, main_pass, sec_host, sec_pass, server_id, symbol, task_col, handler_config)

# =======================
# Main loop
# =======================
def find_secondary_credentials(main_client, symbol_to_find):
    """
    Searches for the specified symbol in CLICKHOUSE_TABLES.
    If found, returns the connection credentials for the row containing it.
    """
    if not symbol_to_find:
        logging.warning("[CredentialFinder] No symbol provided to search for.")
        return None, None
        
    logging.info(f"[CredentialFinder] Searching for secondary DB assigned to symbol: '{symbol_to_find}'...")
    
    search_condition = build_symbol_search_condition(symbol_to_find)
    
    query = f"""
    SELECT CLICKHOUSE_HOST, CLICKHOUSE_PASSWORD 
    FROM {DATABASE_NAME}.{CLICKHOUSE_TABLES}
    WHERE {search_condition}
    LIMIT 1
    """
    
    try:
        rows = main_client.execute(query)
        if rows:
            host, password = rows[0]
            logging.info(f"[CredentialFinder] ✅ Found symbol '{symbol_to_find}' on host: {host.split('.')[0]}")
            return host, password
        else:
            logging.warning(f"[CredentialFinder] ⏳ Symbol '{symbol_to_find}' not found in any DB yet. Will retry.")
            return None, None
            
    except Exception as e:
        logging.error(f"[CredentialFinder] Error finding credentials for '{symbol_to_find}': {e}")
        return None, None

def ensure_servers_table(main_client):
    q = f"""CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{SERVERS_TABLE} (
        server_id String, symbol Nullable(String), task_web_socket Nullable(String),
        statu String DEFAULT 'pending', last_update DateTime DEFAULT now()
    ) ENGINE = MergeTree() ORDER BY server_id"""
    main_client.execute(q)

def main_loop():
    server_id = None
    if os.path.exists(SERVER_ID_FILE):
        with open(SERVER_ID_FILE, 'r') as f:
            server_id = f.read().strip()
    else:
        server_id = rand_id(32)
        with open(SERVER_ID_FILE, 'w') as f: f.write(server_id)
        logging.info(f"[Main] Created and saved new server_id: {server_id}")

    main_client = None
    while True:
        try:
            main_client = get_clickhouse_client(MAIN_CLICKHOUSE_HOST, MAIN_CLICKHOUSE_PASSWORD, main_client)
            ensure_servers_table(main_client)

            if main_client.execute(f"SELECT count() FROM {SERVERS_TABLE} WHERE server_id=%(id)s", {'id': server_id})[0][0] == 0:
                main_client.execute(f"INSERT INTO {SERVERS_TABLE} (server_id, statu) VALUES", [(server_id, '0')])
                logging.info(f"[Main] Registered server: {server_id}")

            rows = main_client.execute(f"SELECT symbol, task_web_socket FROM {SERVERS_TABLE} WHERE server_id = %(id)s", {'id': server_id})
            symbol, task_col = (rows[0] if rows else (None, None))
            logging.info(f"[Main] Read values: symbol='{symbol}', task='{task_col}'")

            main_client.execute(f"ALTER TABLE {SERVERS_TABLE} UPDATE statu = '1', last_update = now() WHERE server_id = %(id)s", {'id': server_id})
            
            if symbol and task_col:
                with active_ws_lock:
                    if server_id not in active_ws:
                        sec_host, sec_pass = find_secondary_credentials(main_client, symbol)
                        
                        if sec_host and sec_pass:
                            logging.info(f"[Main] Credentials found. Starting worker...")
                            thr = start_websocket_worker(
                                MAIN_CLICKHOUSE_HOST, MAIN_CLICKHOUSE_PASSWORD,
                                sec_host, sec_pass, server_id, symbol, task_col
                            )
                            if thr: active_ws[server_id] = thr
                        else:
                            logging.warning(f"[Main] No dedicated DB found for '{symbol}' yet. Waiting for next cycle.")

        except Exception as e:
            logging.error(f"[Main] Main loop exception: {e}", exc_info=True)
            if main_client:
                try: main_client.disconnect()
                except: pass
            main_client = None

        time.sleep(HEARTBEAT_INTERVAL)

if __name__ == "__main__":
    try:
        import websocket
        main_loop()
    except ImportError:
        logging.error("The 'websocket-client' library is not installed. Please install it using: pip install websocket-client")
    except KeyboardInterrupt:
        logging.info("SIGINT received — exiting.")