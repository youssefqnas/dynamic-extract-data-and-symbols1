import os
import random
import string
import time
import threading
from clickhouse_driver import Client, errors

# ==============================
# إعدادات الاتصال
# ==============================
CLICKHOUSE_HOST = "l5bxi83or6.eu-central-1.aws.clickhouse.cloud"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "8aJlVz_A2L4On"
DATABASE_NAME = 'default'
SERVERS_TABLE = 'servers_for_symbols2'
SYMBOLS_TABLE = 'symbols'

# ==============================
# إعدادات التشغيل
# ==============================
TASK_DISCOVERY_INTERVAL = 10  # بالثواني
HEARTBEAT_INTERVAL = 60       # بالثواني
SERVER_IDLE_TIMEOUT = 120     # بالثواني (دقيقتان)
# --- الإضافة الجديدة ---
SERVER_DELETION_TIMEOUT = 60  # بالثواني (دقيقة واحدة) لحذف الصف بعد تحويله لـ 0

TASK_COLUMNS = [
    "task_trade",
    "task_bookTicker",
    "task_markPrice",
    "task_kline_1m",
    "task_kline_3m",
    "task_kline_15m",
    "task_kline_1h"
]

# ==============================
# متغيرات مشتركة
# ==============================
active_assignments = {}  # server_id -> (symbol, task_column)
assignments_lock = threading.Lock()
shutdown_event = threading.Event()

# ==============================
# دوال مساعدة
# ==============================
def get_clickhouse_client(current_client=None):
    if current_client:
        try:
            current_client.execute("SELECT 1")
            return current_client
        except Exception:
            print("[اتصال] إعادة الاتصال...")
    return Client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=DATABASE_NAME,
        secure=True
    )

def verify_or_create_tables(client):
    create_servers_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{SERVERS_TABLE} (
        server_id String,
        symbol Nullable(String),
        task_web_socket Nullable(String),
        statu String DEFAULT 'pending',
        last_update DateTime DEFAULT now()
    ) ENGINE = MergeTree() ORDER BY server_id
    """
    client.execute(create_servers_sql)
    print("[جدول] تم التأكد من وجود جدول السيرفرات.")

    exists_symbols = client.execute(f"EXISTS TABLE {DATABASE_NAME}.{SYMBOLS_TABLE}")[0][0]
    if exists_symbols == 0:
        raise RuntimeError(f"❌ جدول {SYMBOLS_TABLE} غير موجود! يجب إنشاؤه أولاً.")

# ==============================
# Thread لتحديث الـ last_update
# ==============================
def heartbeat_worker(server_id, symbol, task_column):
    client = None
    while not shutdown_event.is_set():
        try:
            client = get_clickhouse_client(client)

            check = client.execute(
                f"""
                SELECT count() FROM {SERVERS_TABLE}
                WHERE server_id = %(id)s
                AND statu = '1'
                AND symbol = %(symbol)s
                AND task_web_socket = %(task)s
                """,
                {"id": server_id, "symbol": symbol, "task": task_column}
            )[0][0]

            if check == 0:
                print(f"[Heartbeat] إيقاف التحديث لـ {server_id} لأنه لم يعد مسؤولاً عن {symbol} ({task_column})")
                break

            client.execute(
                f"""
                ALTER TABLE {SYMBOLS_TABLE}
                UPDATE {task_column}_last_update = now()
                WHERE symbol = %(symbol)s
                """,
                {"symbol": symbol}
            )
            print(f"[Heartbeat] {server_id} ← تحديث {symbol} ({task_column})")

        except Exception as e:
            print(f"[Heartbeat] خطأ في {server_id}: {e}")
            client = None

        shutdown_event.wait(HEARTBEAT_INTERVAL)


# ==============================
# Thread لمراقبة السيرفرات الخاملة
# ==============================
def server_status_watcher():
    client = None
    while not shutdown_event.is_set():
        try:
            client = get_clickhouse_client(client)

            # --- المرحلة الأولى (الكود الأصلي): تحويل السيرفرات الخاملة (1) إلى (0) ---
            # هذا يمنع تحويل السيرفرات الجديدة التي لم تبدأ عملها بعد إلى '0'
            update_query = f"""
                ALTER TABLE {SERVERS_TABLE}
                UPDATE statu = '0', symbol = NULL, task_web_socket = NULL
                WHERE statu = '1' AND now() - last_update > toIntervalSecond({SERVER_IDLE_TIMEOUT})
            """
            client.execute(update_query)
            print("[Watcher] تم التحقق من السيرفرات الخاملة (statu='1') لتحويلها إلى '0'.")

            # --- المرحلة الثانية (الإضافة الجديدة): حذف الصفوف التي حالتها (0) لأكثر من دقيقة ---
            # المنطق: إذا كان الفارق الزمني منذ آخر تحديث (قبل أن يصبح خاملًا) أكبر من
            # مهلة الخمول + مهلة الحذف، فإنه يجب حذفه.
            delete_timeout = SERVER_IDLE_TIMEOUT + SERVER_DELETION_TIMEOUT
            delete_query = f"""
                ALTER TABLE {SERVERS_TABLE}
                DELETE WHERE statu = '0' AND now() - last_update > toIntervalSecond({delete_timeout})
            """
            client.execute(delete_query)
            print(f"[Watcher] تم التحقق من السيرفرات القديمة (statu='0') التي تجاوزت {SERVER_DELETION_TIMEOUT} ثانية لحذفها.")


        except Exception as e:
            print(f"[Watcher] خطأ: {e}")
            client = None

        # يعمل المراقب كل دقيقة
        shutdown_event.wait(60)


# ==============================
# توزيع المهام
# ==============================
def task_discovery_worker():
    client = None
    round_num = 0
    while not shutdown_event.is_set():
        round_num += 1
        print(f"\n===== جولة توزيع رقم {round_num} =====")
        try:
            client = get_clickhouse_client(client)

            servers_query = f"""
                SELECT server_id FROM {SERVERS_TABLE}
                WHERE statu = '1' AND (symbol IS NULL OR task_web_socket IS NULL)
            """
            available_servers = [s[0] for s in client.execute(servers_query)]
            print(f"[جولة {round_num}] عدد السيرفرات المؤهلة: {len(available_servers)}")

            if not available_servers:
                print(f"[جولة {round_num}] لا توجد سيرفرات متاحة. انتظار...")
                shutdown_event.wait(TASK_DISCOVERY_INTERVAL)
                continue

            symbols_query = f"""
                SELECT symbol, {', '.join(TASK_COLUMNS)}
                FROM {SYMBOLS_TABLE}
                ORDER BY hot_rank ASC
            """
            all_symbols_with_tasks = client.execute(symbols_query)

            assigned_symbols_this_round = set()

            for row in all_symbols_with_tasks:
                if not available_servers:
                    print(f"[جولة {round_num}] نفدت السيرفرات المتاحة. إنهاء الجولة.")
                    break

                symbol = row[0]
                if symbol in assigned_symbols_this_round:
                    continue

                tasks_status = row[1:]
                for idx, val in enumerate(tasks_status):
                    if val == 0:
                        server_id = available_servers.pop(0)
                        task_column = TASK_COLUMNS[idx]

                        print(f"[جولة {round_num}] {server_id} ← تخصيص {symbol} ({task_column})")

                        client.execute(
                            f"""
                            ALTER TABLE {SYMBOLS_TABLE}
                            UPDATE {task_column} = 1, {task_column}_last_update = now()
                            WHERE symbol = %(symbol)s AND {task_column} = 0
                            """,
                            {"symbol": symbol}
                        )

                        client.execute(
                            f"""
                            ALTER TABLE {SERVERS_TABLE}
                            UPDATE symbol = %(symbol)s, task_web_socket = %(task)s
                            WHERE server_id = %(id)s
                            """,
                            {"symbol": symbol, "task": task_column, "id": server_id}
                        )

                        assigned_symbols_this_round.add(symbol)
                        with assignments_lock:
                            active_assignments[server_id] = (symbol, task_column)
                        threading.Thread(
                            target=heartbeat_worker,
                            args=(server_id, symbol, task_column),
                            daemon=True
                        ).start()

                        break

            print(f"[جولة {round_num}] انتهاء الجولة. انتظار {TASK_DISCOVERY_INTERVAL} ثانية...")

        except Exception as e:
            print(f"[توزيع المهام] خطأ: {e}")
            client = None

        shutdown_event.wait(TASK_DISCOVERY_INTERVAL)

# ==============================
# تشغيل البرنامج
# ==============================
if __name__ == "__main__":
    try:
        main_client = get_clickhouse_client()
        verify_or_create_tables(main_client)
        main_client.disconnect()

        print("\n--- بدء خدمات توزيع المهام والمراقبة ---")

        task_distributor_thread = threading.Thread(target=task_discovery_worker, daemon=True)
        task_distributor_thread.start()

        watcher_thread = threading.Thread(target=server_status_watcher, daemon=True)
        watcher_thread.start()

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[إيقاف] تم استقبال إشارة الإنهاء... جاري إيقاف الـ threads.")
        shutdown_event.set()
    except Exception as e:
        print(f"[خطأ رئيسي] حدث خطأ فادح: {e}")
        shutdown_event.set()