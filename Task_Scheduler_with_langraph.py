"""
SQLite Task Scheduler + NEXA Email Agent — CRASH-PROTECTED VERSION
===================================================================

CRASH-PROTECTION:
1. Signal handlers (SIGTERM, SIGBREAK, CTRL_CLOSE) — logs WHY process was killed
2. atexit handler — logs on ANY exit path
3. threading.excepthook — catches unhandled exceptions in daemon threads
4. sys.excepthook — catches unhandled exceptions on main thread
5. Non-daemon keepalive thread — prevents silent exit if main thread dies
6. Broad main-loop exception handling (catches BaseException)
7. Single SQLite database for scheduler configuration + status; WAL and busy timeout
8. Window-Aware Log Watch: Stop escalation outside WINDOW_END
9. KEEP_INBOX_N_DAYS: Delete emails before today-N calendar days (e.g. NEXA:KEEP_INBOX_8_DAYS)
10.Strict No-Retry: Cannot retry if window closed

BUG FIXES (Aug 2026):
B1. _watch_log_after_spawn: STEP1/STEP2 diagnostic logs before every DB call.
    _write_log_times failure is now WARNING (non-fatal) — thread no longer dies
    silently. print()+log() in ALL except blocks so errors surface even if log
    file is locked. Per-run escalation guard (run_guard_key) not per-slot.
    Poll heartbeat every 10 polls (~100s) — thread-alive always provable.
B2. scheduler_loop: _detect_and_clear_schedule_changes() detects WINDOW_START /
    INTERVAL_MIN changes and immediately clears stale _fired_slots so new
    schedule takes effect within 1s of cache refresh (not at midnight).

Database  : C:\\Data\\Kaaleedass\\scheduler.db
Logs      : T:\\INTERNAL\\Kaaleedass\\config\\Logs\\TASK_MANAGER_LOG\\

pip install psutil pywin32
"""

import os
import re
import sys
import time
import signal
import atexit
import socket
import psutil
import sqlite3
import subprocess
import threading
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import TypedDict, Any

try:
    from langgraph.graph import StateGraph, END
except ImportError as exc:
    raise ImportError(
        "LangGraph is required for the rule-based scheduler. Install it with: "
        "python -m pip install langgraph"
    ) from exc

import win32com.client as _win32

# ─── Heartbeat file ────────────────────────────────────────────────────────────

HEARTBEAT_FILE = r"C:\Users\Kaale\Desktop\MY_Personal_project\Task_scheduler_agent\TASK_TABLE\heart_beat\agent_heart_beat.txt"

# PURPOSE: Refreshes a heartbeat text file so another process can tell the scheduler is alive.
def update_heartbeat():
    try:
        folder = os.path.dirname(HEARTBEAT_FILE)
        if not os.path.exists(folder):
            try:
                os.makedirs(folder, exist_ok=True)
                log(f"[HEARTBEAT] Created folder: {folder}")
            except PermissionError:
                log(f"[HEARTBEAT] ❌ PERMISSION DENIED — Cannot create {folder}")
                return
            except Exception as e:
                log(f"[HEARTBEAT] ❌ Cannot create folder {folder}: {e}")
                return
        timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        try:
            with open(HEARTBEAT_FILE, "w", encoding="utf-8") as f:
                f.write(timestamp)
        except PermissionError:
            log(f"[HEARTBEAT] ❌ PERMISSION DENIED — Cannot write to {HEARTBEAT_FILE}")
        except FileNotFoundError:
            log(f"[HEARTBEAT] ❌ FILE PATH ERROR — Path does not exist: {HEARTBEAT_FILE}")
        except Exception as e:
            log(f"[HEARTBEAT] ❌ Cannot write to {HEARTBEAT_FILE}: {e}")
    except Exception as e:
        log(f"[HEARTBEAT] ❌ Unexpected error: {e}")

# ─── Configuration ─────────────────────────────────────────────────────────────

# One local SQLite database stores ALL scheduler tables.
# Local disk is strongly recommended; do not place a WAL database on a network share.
DB_DIR = r"C:\Users\Kaale\Desktop\MY_Personal_project\Task_scheduler_agent\TASK_TABLE"
SQLITE_FILE = os.path.join(DB_DIR, "scheduler.db")
LOG_DIR = r"C:\Users\Kaale\Desktop\Log\Task_manager_log"

NEXA_SCAN_LIMIT    = 15
NEXA_POLL_INTERVAL = 60

# ─── Globals ───────────────────────────────────────────────────────────────────

APP_RUNNING = True
running_processes: dict = {}
processes_lock          = threading.Lock()
db_lock                 = threading.Lock()

_cpu_count   = psutil.cpu_count(logical=False) or 4
_max_workers = min(_cpu_count * 3, 24)
executor     = ThreadPoolExecutor(max_workers=_max_workers)
print(f"[INIT] ThreadPoolExecutor: {_max_workers} workers ({_cpu_count} cores)")

# ✅ CRITICAL FIX: Dedicated executor ONLY for task launching
# DB operations (upsert_status, merge_parallel) use 'executor' above.
# Task launches use '_launch_executor' which is NEVER blocked by DB threads.
# This prevents thread pool saturation from stopping scheduled task launches.
_launch_executor = ThreadPoolExecutor(max_workers=32, thread_name_prefix="TaskLaunch")
print("[INIT] Dedicated launch executor: 32 workers (isolated from DB threads)")

# ─── Logging ───────────────────────────────────────────────────────────────────

_log_lock = threading.Lock()

# PURPOSE: Prints a timestamped message and appends it to the daily log file.
def log(msg: str):
    line = f"[{datetime.now():%d-%m-%Y %H:%M:%S}] {msg}"
    print(line, flush=True)
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        log_file = os.path.join(LOG_DIR, f"TASK_MANAGER_LOG_{datetime.now():%d-%m-%Y}.log")
        with _log_lock:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        print(f"[LOG FILE ERROR] {e}", flush=True)

# ─── Crash Protection ──────────────────────────────────────────────────────────

# PURPOSE: Stops the scheduler cleanly and shuts down its thread pools.
def _shutdown(reason: str):
    global APP_RUNNING
    if not APP_RUNNING:
        return
    APP_RUNNING = False
    log(f"[SHUTDOWN] ⚠️ Process terminating — reason: {reason}")
    log(f"[SHUTDOWN] PID={os.getpid()}, Host={socket.gethostname()}")
    try:
        executor.shutdown(wait=False, cancel_futures=True)
        _launch_executor.shutdown(wait=False, cancel_futures=True)
        log("[SHUTDOWN] Executor shut down")
    except Exception as e:
        log(f"[SHUTDOWN] Executor shutdown error: {e}")
    log(f"[SHUTDOWN] ✅ Cleanup complete — {reason}")

# PURPOSE: Handles operating-system shutdown/interrupt signals.
def _signal_handler(signum, frame):
    sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
    _shutdown(f"Signal received: {sig_name} ({signum})")
    sys.exit(0)

# PURPOSE: Writes a final log message when Python exits normally.
def _atexit_handler():
    log("[ATEXIT] Python interpreter exiting — atexit handler fired")

# PURPOSE: Logs an uncaught exception from the main thread, then starts shutdown.
def _main_excepthook(exc_type, exc_value, exc_tb):
    tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    log(f"[FATAL] Unhandled exception on MAIN thread:\n{tb_str}")
    _shutdown(f"Unhandled main-thread exception: {exc_type.__name__}: {exc_value}")

# PURPOSE: Logs an uncaught exception from a background thread.
def _thread_excepthook(args):
    tb_str = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
    thread_name = args.thread.name if args.thread else "unknown"
    log(f"[FATAL] Unhandled exception in thread '{thread_name}':\n{tb_str}")

# PURPOSE: Registers signal handlers and exception hooks for diagnostics.
def _install_crash_protection():
    signal.signal(signal.SIGINT,  _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _signal_handler)
    try:
        import win32api
        def _console_handler(ctrl_type):
            type_names = {0:"CTRL_C_EVENT", 1:"CTRL_BREAK_EVENT", 2:"CTRL_CLOSE_EVENT",
                          5:"CTRL_LOGOFF_EVENT", 6:"CTRL_SHUTDOWN_EVENT"}
            name = type_names.get(ctrl_type, f"UNKNOWN({ctrl_type})")
            _shutdown(f"Windows console event: {name}")
            return True
        win32api.SetConsoleCtrlHandler(_console_handler, True)
        log("[INIT] ✅ win32api console handler installed (CTRL_CLOSE/LOGOFF/SHUTDOWN)")
    except ImportError:
        log("[INIT] ⚠️ win32api not available — console close/logoff events won't be caught")
    except Exception as e:
        log(f"[INIT] ⚠️ Console handler install failed: {e}")
    atexit.register(_atexit_handler)
    sys.excepthook = _main_excepthook
    if hasattr(threading, "excepthook"):
        threading.excepthook = _thread_excepthook
    log("[INIT] ✅ Crash-protection installed (signals, atexit, excepthooks)")

# ─── Checked Sleep ─────────────────────────────────────────────────────────────

# PURPOSE: Sleeps in small chunks so shutdown requests can interrupt the wait.
def _sleep_checked(seconds: float, chunk: float = 5.0) -> bool:
    while seconds > 0 and APP_RUNNING:
        time.sleep(min(seconds, chunk))
        seconds -= chunk
    return not APP_RUNNING

# ─── Database helpers ──────────────────────────────────────────────────────────

DB_RETRY_ATTEMPTS = 20
DB_RETRY_DELAY    = 1.0


# PURPOSE: Recognizes SQLite lock/busy errors so database operations can retry.
def _is_lock_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "database is locked" in msg or "database is busy" in msg or "locked" in msg


# PURPOSE: Opens the one SQLite database and enables WAL, busy timeout and foreign keys.
def _sqlite_conn():
    """Open the unified SQLite database with WAL and a bounded lock wait."""
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(SQLITE_FILE, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# PURPOSE: Compatibility wrapper that returns a SQLite connection.
def get_conn():
    """Compatibility helper: all former Access calls now connect to SQLite."""
    return _sqlite_conn()


# PURPOSE: Runs a database callback, commits or rolls back, and retries lock errors.
def _run_with_retry(fn):
    """Run a DB operation with short lock retries and guaranteed connection cleanup."""
    last_exc = None
    for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
        conn = None
        try:
            conn = _sqlite_conn()
            cur = conn.cursor()
            result = fn(conn, cur)
            conn.commit()
            return result
        except Exception as e:
            last_exc = e
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if _is_lock_error(e) and attempt < DB_RETRY_ATTEMPTS:
                if attempt == 1:
                    log("[DB] SQLite is busy; retrying with busy timeout...")
                time.sleep(DB_RETRY_DELAY)
                continue
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    raise last_exc


# PURPOSE: Creates missing scheduler tables/indexes while preserving existing rows.
def _init_sqlite():
    """Create every scheduler table if missing; preserve all existing rows."""
    try:
        with _sqlite_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS TASK_MASTER (
                    TASK_ID INTEGER PRIMARY KEY,
                    TASK_NAME TEXT NOT NULL DEFAULT '',
                    SCRIPT_PATH TEXT DEFAULT '',
                    ENABLED TEXT DEFAULT 'Y',
                    SCHEDULE_TYPE TEXT DEFAULT 'INTERVAL',
                    SCHEDULE_TIME TEXT DEFAULT '',
                    INTERVAL_MIN INTEGER DEFAULT 60,
                    SCHEDULE_MODE TEXT DEFAULT 'FIXED',
                    MANUAL_ACTION TEXT,
                    MAX_RUNTIME_MIN INTEGER DEFAULT 60,
                    ALERT_MAIL TEXT DEFAULT '',
                    WINDOW_START TEXT DEFAULT '',
                    WINDOW_END TEXT DEFAULT '',
                    LOG_CHECK TEXT DEFAULT 'AUTO',
                    MINUTES INTEGER DEFAULT 10,
                    LOG_PATH TEXT DEFAULT '',
                    UPDATED_TIME TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS AGENT_MODE (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    AGENT_MODE TEXT DEFAULT 'DISABLE',
                    OUTBOX_CHECK TEXT DEFAULT 'MANUAL',
                    EMAIL_ACCESS TEXT DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS TASK_STATUS (
                    RUN_KEY TEXT PRIMARY KEY,
                    TASK_ID INTEGER,
                    TASK_NAME TEXT,
                    STATUS TEXT,
                    TRIGGER TEXT,
                    LAST_RUN TEXT,
                    NEXT_RUN TEXT,
                    DURATION_SEC REAL,
                    PID INTEGER,
                    MESSAGE TEXT,
                    UPDATED_TIME TEXT
                );
                CREATE TABLE IF NOT EXISTS FIRED_SLOTS (
                    slot_key TEXT PRIMARY KEY,
                    fired_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS LOG_PATHS (
                    TASK_ID INTEGER PRIMARY KEY,
                    LOG_PATH TEXT DEFAULT '',
                    Created_time TEXT,
                    LOG_UPDATED TEXT
                );
                CREATE TABLE IF NOT EXISTS TASK_MASTER_LOCAL (
                    TASK_ID INTEGER PRIMARY KEY,
                    TASK_NAME TEXT, SCHEDULE_TYPE TEXT, SCHEDULE_TIME TEXT,
                    SCHEDULE_MODE TEXT, INTERVAL_MIN INTEGER, WINDOW_START TEXT,
                    WINDOW_END TEXT, LOG_CHECK TEXT, MINUTES INTEGER,
                    MAX_RUNTIME_MIN INTEGER, SCRIPT_PATH TEXT, LOG_PATH TEXT,
                    ENABLED TEXT, UPDATED_TIME TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_task_id ON TASK_STATUS(TASK_ID);
                CREATE INDEX IF NOT EXISTS idx_updated ON TASK_STATUS(UPDATED_TIME);
                CREATE INDEX IF NOT EXISTS idx_local_id ON TASK_MASTER_LOCAL(TASK_ID);
            """)
            # Ensure columns exist in databases created by earlier versions.
            _ensure_column(conn, "TASK_MASTER", "LOG_PATH", "TEXT DEFAULT ''")
            _ensure_column(conn, "TASK_MASTER", "ALERT_MAIL", "TEXT DEFAULT ''")
            _ensure_column(conn, "AGENT_MODE", "OUTBOX_CHECK", "TEXT DEFAULT 'MANUAL'")
            _ensure_column(conn, "AGENT_MODE", "EMAIL_ACCESS", "TEXT DEFAULT ''")
            _ensure_column(conn, "LOG_PATHS", "LOG_UPDATED", "TEXT")
            row = conn.execute("SELECT AGENT_MODE FROM AGENT_MODE LIMIT 1").fetchone()
            if row is None:
                conn.execute("INSERT INTO AGENT_MODE (AGENT_MODE, OUTBOX_CHECK) VALUES ('DISABLE','MANUAL')")
        log(f"[INIT] SQLite ready (TASK_MASTER + AGENT_MODE + TASK_STATUS + FIRED_SLOTS) -> {SQLITE_FILE}")
    except Exception as e:
        log(f"[INIT] SQLite init error: {e}")
        raise


# PURPOSE: Adds a column only when an older database schema does not already have it.
def _ensure_column(conn, table: str, column: str, definition: str):
    existing = {str(row[1]).upper() for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column.upper() not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


# PURPOSE: Executes a callback using a managed SQLite connection with lock retries.
def _sqlite_run(fn):
    """Run a function against the unified SQLite database."""
    last_exc = None
    for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
        try:
            with _sqlite_conn() as conn:
                result = fn(conn)
                conn.commit()
                return result
        except Exception as e:
            last_exc = e
            if _is_lock_error(e) and attempt < DB_RETRY_ATTEMPTS:
                time.sleep(DB_RETRY_DELAY)
                continue
            log(f"[SQLITE] Error: {e}")
            raise
    raise last_exc


# PURPOSE: Copies the currently loaded task definitions into a backup table in the same DB.
def _save_task_master_backup(tasks: list):
    """
    Save a compatibility backup copy of TASK_MASTER inside the same SQLite database.
    This is not a second database and is not required for normal task loading.
    """
    def _safe_int(val, default=None):
        """Convert to int safely — returns default if val is None or invalid."""
        if val is None:
            return default
        try:
            return int(val)
        except (TypeError, ValueError):
            return default

    def _safe_str(val):
        """Convert to str safely — returns empty string if val is None."""
        return str(val) if val is not None else ""

    def _fn(conn):
        conn.execute("DELETE FROM TASK_MASTER_LOCAL")
        for task in tasks:
            conn.execute("""
                INSERT OR REPLACE INTO TASK_MASTER_LOCAL
                (TASK_ID, TASK_NAME, SCHEDULE_TYPE, SCHEDULE_TIME, SCHEDULE_MODE,
                 INTERVAL_MIN, WINDOW_START, WINDOW_END, LOG_CHECK, MINUTES,
                 MAX_RUNTIME_MIN, SCRIPT_PATH, LOG_PATH, ENABLED, UPDATED_TIME)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                _safe_int(task.get("TASK_ID"), 0),
                _safe_str(task.get("TASK_NAME")),
                _safe_str(task.get("SCHEDULE_TYPE")),
                _safe_str(task.get("SCHEDULE_TIME")),
                _safe_str(task.get("SCHEDULE_MODE")),
                _safe_int(task.get("INTERVAL_MIN")),       # None is OK
                _safe_str(task.get("WINDOW_START")),
                _safe_str(task.get("WINDOW_END")),
                _safe_str(task.get("LOG_CHECK")),
                _safe_int(task.get("MINUTES")),             # None is OK
                _safe_int(task.get("MAX_RUNTIME_MIN"), 60), # default 60
                _safe_str(task.get("SCRIPT_PATH")),
                _safe_str(task.get("LOG_PATH")),
                _safe_str(task.get("ENABLED")) or "Y",
                datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            ))
    try:
        _sqlite_run(_fn)
    except Exception as e:
        log(f"[BACKUP] Failed to save task backup: {e}")


# PURPOSE: Loads task definitions from the local SQLite backup table.
def _load_task_master_backup() -> list:
    """
    Load the compatibility backup of task definitions from the same SQLite database.
    """
    def _fn(conn):
        rows = conn.execute("""
            SELECT TASK_ID, TASK_NAME, SCHEDULE_TYPE, SCHEDULE_TIME, SCHEDULE_MODE,
                   INTERVAL_MIN, WINDOW_START, WINDOW_END, LOG_CHECK, MINUTES,
                   MAX_RUNTIME_MIN, SCRIPT_PATH, LOG_PATH, ENABLED
            FROM TASK_MASTER_LOCAL
            ORDER BY TASK_ID
        """).fetchall()
        tasks = []
        for row in rows:
            task = {
                "TASK_ID": row[0],
                "TASK_NAME": row[1],
                "SCHEDULE_TYPE": row[2],
                "SCHEDULE_TIME": row[3],
                "SCHEDULE_MODE": row[4],
                "INTERVAL_MIN": row[5],
                "WINDOW_START": row[6],
                "WINDOW_END": row[7],
                "LOG_CHECK": row[8],
                "MINUTES": row[9],
                "MAX_RUNTIME_MIN": row[10],
                "SCRIPT_PATH": row[11],
                "LOG_PATH": row[12],
                "ENABLED": row[13],
            }
            tasks.append(task)
        return tasks
    try:
        return _sqlite_run(_fn)
    except Exception as e:
        log(f"[BACKUP] Failed to load task backup: {e}")
        return []


# PURPOSE: Loads enabled task configuration from the scheduler database.
def load_tasks() -> list[dict]:
    """Load enabled task definitions directly from the unified SQLite database."""
    def _fn(conn, cur):
        cur.execute("SELECT * FROM TASK_MASTER WHERE UPPER(COALESCE(ENABLED,'Y'))='Y' ORDER BY TASK_ID")
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    with db_lock:
        return _run_with_retry(_fn)

# PURPOSE: Clears a manual-action request after it has been handled.
def clear_manual_action(task_id):
    def _fn(conn, cur):
        cur.execute("UPDATE TASK_MASTER SET MANUAL_ACTION=NULL WHERE TASK_ID=?", (int(task_id),))
    with db_lock:
        _run_with_retry(_fn)

# PURPOSE: Resets task-status records that were left as running after an abnormal stop.
def reset_stale_running():
    def _fn(conn, cur):
        cur.execute(
            "UPDATE TASK_STATUS SET STATUS='CRASHED', MESSAGE='Reset on startup' WHERE STATUS='RUNNING'"
        )
        return cur.rowcount
    with db_lock:
        count = _run_with_retry(_fn)
    if count:
        log(f"[STARTUP] Reset {count} stale RUNNING row(s) to CRASHED.")

# ─── AGENT_MODE helpers ────────────────────────────────────────────────────────

# PURPOSE: Ensures the agent configuration table and its expected columns exist.
def _ensure_agent_mode_table():
    def _fn(conn, cur):
        try:
            cur.execute("SELECT AGENT_MODE FROM AGENT_MODE LIMIT 1")
            return "EXISTS"
        except Exception:
            pass
        try:
            cur.execute("CREATE TABLE AGENT_MODE (AGENT_MODE TEXT)")
            cur.execute("INSERT INTO AGENT_MODE VALUES ('DISABLE')")
            return "CREATED"
        except Exception as e:
            return f"ERROR: {e}"
    try:
        with db_lock:
            result = _run_with_retry(_fn)
        if result == "CREATED":
            log("[STARTUP] AGENT_MODE table created (default: DISABLE)")
        elif result == "EXISTS":
            log("[STARTUP] AGENT_MODE table found")
            _init_sqlite()
            
            # ✅ Restore today's fired slots from SQLite
            # Prevents re-firing tasks that already ran today if scheduler restarts
            restored = _restore_fired_slots()
            if restored:
                with _fired_lock:
                    _fired_slots.update(restored)
                log(f"[STARTUP] Restored {len(restored)} fired slot(s) from SQLite — prevents duplicate launches")
            
            # Optional compatibility backup remains in the same SQLite database.
            backup_tasks = _load_task_master_backup()
            if backup_tasks:
                with _cache_lock:
                    _cached_tasks = backup_tasks
                log(f"[STARTUP] Loaded {len(backup_tasks)} task(s) from SQLite backup (database may be unavailable)")
        else:
            log(f"[STARTUP] {result}")
    except Exception as e:
        log(f"[STARTUP] AGENT_MODE table setup error: {e}")

# PURPOSE: Returns whether the configured agent mode is enabled.
def is_agent_enabled() -> bool:
    def _fn(conn, cur):
        cur.execute("SELECT AGENT_MODE FROM AGENT_MODE LIMIT 1")
        row = cur.fetchone()
        if not row:
            return False
        return str(row[0] or "").strip().upper() == "ENABLE"
    try:
        with db_lock:
            return _run_with_retry(_fn)
    except Exception:
        return False

# PURPOSE: Reads the current agent configuration as a dictionary.
def _read_agent_mode_row() -> dict:
    def _fn(conn, cur):
        cur.execute("SELECT * FROM AGENT_MODE")
        cols = [c[0].upper() for c in cur.description]
        row  = cur.fetchone()
        if not row:
            return {}
        return {cols[i]: str(row[i] or "").strip() for i in range(len(cols))}
    try:
        with db_lock:
            return _run_with_retry(_fn)
    except Exception:
        return {}

# PURPOSE: Updates the email outbox checking mode in SQLite.
def _set_outbox_check_mode(mode: str):
    def _fn(conn, cur):
        cur.execute("UPDATE AGENT_MODE SET OUTBOX_CHECK=?", (mode.upper(),))
    try:
        with db_lock:
            _run_with_retry(_fn)
        log(f"[OUTBOX_AUTO] OUTBOX_CHECK set to {mode.upper()} in AGENT_MODE table")
    except Exception as e:
        log(f"[OUTBOX_AUTO] Could not update OUTBOX_CHECK: {e}")

# PURPOSE: Checks an email sender against the configured allow-list.
def _is_sender_allowed(sender_email: str) -> bool:
    row = _read_agent_mode_row()
    allowed_raw = row.get("EMAIL_ACCESS", "").strip()
    if not allowed_raw:
        return True
    allowed_list = [
        a.strip().lower()
        for a in allowed_raw.replace(",", ";").split(";")
        if a.strip()
    ]
    return str(sender_email or "").strip().lower() in allowed_list

# ─── Status upsert helpers ─────────────────────────────────────────────────────

# PURPOSE: Inserts or updates a task's current status, process ID and message.
def _upsert_status_row(run_key: str, task: dict, status: str,
                       message: str = "", pid=None, next_run: str = "",
                       duration=None, trigger: str = "SCHEDULED"):
    """Write task status to SQLite (local, fast, no network locks)."""
    task_id   = int(task["TASK_ID"])
    task_name = str(task.get("TASK_NAME", ""))
    now       = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    def _fn(conn):
        existing = conn.execute(
            "SELECT RUN_KEY FROM TASK_STATUS WHERE RUN_KEY=?", (run_key,)
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE TASK_STATUS SET
                    STATUS=?, MESSAGE=?, NEXT_RUN=?, DURATION_SEC=?,
                    PID=?, UPDATED_TIME=?
                   WHERE RUN_KEY=?""",
                (status, message, next_run,
                 float(duration) if duration not in ("", None) else None,
                 int(pid) if pid not in ("", None) else None,
                 now, run_key)
            )
        else:
            # Check if any row for this task_id exists
            existing_task = conn.execute(
                "SELECT RUN_KEY FROM TASK_STATUS WHERE TASK_ID=?", (task_id,)
            ).fetchone()
            if existing_task:
                conn.execute(
                    """UPDATE TASK_STATUS SET
                        RUN_KEY=?, TASK_NAME=?, STATUS=?, TRIGGER=?,
                        LAST_RUN=?, NEXT_RUN=?, DURATION_SEC=?, PID=?,
                        MESSAGE=?, UPDATED_TIME=?
                       WHERE TASK_ID=?""",
                    (run_key, task_name, status, trigger,
                     now, next_run,
                     float(duration) if duration not in ("", None) else None,
                     int(pid) if pid not in ("", None) else None,
                     message, now, task_id)
                )
            else:
                conn.execute(
                    """INSERT INTO TASK_STATUS
                        (RUN_KEY,TASK_ID,TASK_NAME,STATUS,TRIGGER,
                         LAST_RUN,NEXT_RUN,DURATION_SEC,PID,MESSAGE,UPDATED_TIME)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_key, task_id, task_name, status, trigger,
                     now, next_run,
                     float(duration) if duration not in ("", None) else None,
                     int(pid) if pid not in ("", None) else None,
                     message, now)
                )

    try:
        _sqlite_run(_fn)
    except Exception as e:
        log(f"[STATUS ERROR] {task_name}: {e}")


# PURPOSE: Combines status rows after parallel executions of the same task finish.
def _merge_parallel_rows(task: dict, finished_run_key: str, final_status: str,
                         final_message: str, pid, duration, next_run: str):
    task_id   = int(task["TASK_ID"])
    task_name = str(task.get("TASK_NAME", ""))
    now       = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    with processes_lock:
        still_running_keys = {
            v["run_key"]
            for v in running_processes.values()
            if str(v["task"]["TASK_ID"]) == str(task_id)
        }

    def _fn(conn):
        rows = conn.execute(
            "SELECT RUN_KEY,STATUS,LAST_RUN,DURATION_SEC,PID,MESSAGE FROM TASK_STATUS WHERE TASK_ID=? ORDER BY LAST_RUN",
            (task_id,)
        ).fetchall()
        if not rows:
            return

        duration_val = float(duration) if duration not in ("", None) else None
        pid_val      = int(pid)        if pid      not in ("", None) else None
        conn.execute(
            """UPDATE TASK_STATUS SET
                STATUS=?, DURATION_SEC=?, PID=?, MESSAGE=?, NEXT_RUN=?, UPDATED_TIME=?
               WHERE RUN_KEY=?""",
            (final_status, duration_val, pid_val, final_message, next_run, now, finished_run_key)
        )

        if still_running_keys:
            log(f"[PARALLEL] {task_name}: {len(still_running_keys)} instance(s) still running.")
            return

        all_rows = conn.execute(
            "SELECT RUN_KEY,STATUS,LAST_RUN,DURATION_SEC,PID,MESSAGE FROM TASK_STATUS WHERE TASK_ID=? ORDER BY LAST_RUN",
            (task_id,)
        ).fetchall()
        if len(all_rows) <= 1:
            return

        parts = []
        for r in all_rows:
            rk, st, lr, dur, p2, msg = r
            dur_str = f"{float(dur):.1f}s" if dur else "?"
            parts.append(f"[{rk}] {st} in {dur_str} | {msg}")
        merged_msg  = " || ".join(parts)
        keep_key    = all_rows[-1][0]
        delete_keys = [r[0] for r in all_rows if r[0] != keep_key]
        for dk in delete_keys:
            conn.execute("DELETE FROM TASK_STATUS WHERE RUN_KEY=?", (dk,))
        conn.execute(
            """UPDATE TASK_STATUS SET STATUS=?, DURATION_SEC=?, PID=?, MESSAGE=?, NEXT_RUN=?, UPDATED_TIME=?
               WHERE RUN_KEY=?""",
            (final_status, duration_val, pid_val, f"All runs complete: {merged_msg}", next_run, now, keep_key)
        )
        log(f"[PARALLEL] {task_name}: all instances finished — merged {len(all_rows)} rows → 1.")

    try:
        _sqlite_run(_fn)
    except Exception as e:
        log(f"[MERGE ERROR] {task_name}: {e}")

# ─── Schedule helpers ──────────────────────────────────────────────────────────

# PURPOSE: Parses a clock value such as '09:35' into hour and minute integers.
def _parse_hhmm(raw) -> tuple | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        h, m = map(int, s.split(":"))
        return (h, m)
    except ValueError:
        return None

# PURPOSE: Checks whether the current clock time is inside the task's configured window.
def in_window(task) -> bool:
    start = _parse_hhmm(task.get("WINDOW_START"))
    end   = _parse_hhmm(task.get("WINDOW_END"))
    if not start and not end:
        return True
    now_t   = datetime.now().hour * 60 + datetime.now().minute
    start_t = (start[0] * 60 + start[1]) if start else 0
    end_t   = (end[0]   * 60 + end[1])   if end   else 23 * 60 + 59
    if start_t <= end_t:
        inside = start_t <= now_t <= end_t
    else:
        inside = now_t >= start_t or now_t <= end_t
    return inside

# PURPOSE: Calculates the next interval-aligned time from a fixed window start.
def calc_next_fixed_slot(interval_min: int, window_start: tuple, now: datetime) -> datetime | None:
    if not window_start:
        return None
    base = now.replace(hour=window_start[0], minute=window_start[1], second=0, microsecond=0)
    if base > now:
        return base
    elapsed_min  = (now - base).total_seconds() / 60
    slots_passed = int(elapsed_min // interval_min) + 1
    return base + timedelta(minutes=interval_min * slots_passed)

# PURPOSE: Calculates the next expected run time for daily, interval or fixed-time schedules.
def calc_next_run(task, after: datetime | None = None) -> datetime | None:
    base          = after or datetime.now()
    schedule_type = str(task.get("SCHEDULE_TYPE", "MANUAL")).upper().strip()
    schedule_mode = str(task.get("SCHEDULE_MODE", "ROLLING")).upper().strip()

    if schedule_type == "DAILY":
        raw = str(task.get("SCHEDULE_TIME", "00:00")).strip()
        try:
            h, m = map(int, raw.split(":"))
        except ValueError:
            return None
        candidate = base.replace(hour=h, minute=m, second=0, microsecond=0)
        if candidate <= base:
            candidate += timedelta(days=1)
        return candidate

    elif schedule_type == "INTERVAL":
        if schedule_mode == "FIXED_TIMES":
            times_str = str(task.get("SCHEDULE_TIME") or "").strip()
            if not times_str:
                return None
            try:
                times_hm = [_parse_hhmm(t.strip()) for t in times_str.split(",")]
                times_hm = [x for x in times_hm if x]
            except Exception:
                return None
            if not times_hm:
                return None
            now_min      = base.hour * 60 + base.minute
            sorted_times = sorted(times_hm, key=lambda x: x[0] * 60 + x[1])
            for hm in sorted_times:
                if hm[0] * 60 + hm[1] > now_min:
                    return base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)
            first = sorted_times[0]
            return (base + timedelta(days=1)).replace(hour=first[0], minute=first[1], second=0, microsecond=0)

        try:
            interval = int(task.get("INTERVAL_MIN"))
        except (TypeError, ValueError):
            return None
        if schedule_mode == "FIXED":
            ws = _parse_hhmm(task.get("WINDOW_START"))
            return calc_next_fixed_slot(interval, ws, base)
        else:
            return base + timedelta(minutes=interval)

    return None

# PURPOSE: Applies deterministic rules to decide whether a task should run at this minute.
def is_due(task, last_run_str: str, now: datetime = None) -> bool:
    # ✅ BUG FIX: Accept 'now' as parameter so scheduler_loop can pass the SAME
    # datetime it uses for slot_key. Previously is_due() called datetime.now()
    # internally, which could return a DIFFERENT minute than the outer 'now' when
    # called near a minute boundary (e.g. outer now=03:59, is_due now=04:00).
    # This caused two different slot_keys ('03:59' and '04:00') to be generated
    # for the same task in consecutive ticks → task launched TWICE per minute.
    if now is None:
        now = datetime.now()

    name          = task.get("TASK_NAME", f"ID={task.get('TASK_ID')}")
    schedule_type = str(task.get("SCHEDULE_TYPE") or "").upper().strip()
    schedule_mode = str(task.get("SCHEDULE_MODE") or "ROLLING").upper().strip()

    if not schedule_type or schedule_type == "MANUAL":
        return False
    if not in_window(task):
        return False

    if schedule_type == "DAILY":
        raw_time = str(task.get("SCHEDULE_TIME") or "").strip()
        if not raw_time:
            return False
        try:
            h, m = map(int, raw_time.split(":"))
        except ValueError:
            return False
        if now.hour != h or now.minute != m:
            return False
        if last_run_str:
            try:
                last = datetime.strptime(last_run_str, "%d-%m-%Y %H:%M:%S")
                if last.date() == now.date() and last.hour == h and last.minute == m:
                    return False
            except ValueError:
                pass
        return True

    elif schedule_type == "INTERVAL":

        # ✅ FIX: If INTERVAL_MIN is NULL but SCHEDULE_TIME has comma-separated
        # times (e.g. "11:35,13:35,15:35"), treat as FIXED_TIMES regardless of mode.
        # This handles "Target vs Actual" style tasks that have specific fire times
        # but no interval configured.
        schedule_time_raw = str(task.get("SCHEDULE_TIME") or "").strip()
        if not task.get("INTERVAL_MIN") and "," in schedule_time_raw:
            # Route to FIXED_TIMES logic
            try:
                times_hm = [_parse_hhmm(t.strip()) for t in schedule_time_raw.split(",")]
                times_hm = [x for x in times_hm if x]
            except Exception:
                return False
            if not times_hm:
                return False
            now_hm = (now.hour, now.minute)
            if now_hm not in times_hm:
                return False
            if last_run_str:
                try:
                    last = datetime.strptime(last_run_str, "%d-%m-%Y %H:%M:%S")
                    if last.date() == now.date() and last.hour == now.hour and last.minute == now.minute:
                        return False
                except ValueError:
                    pass
            return True

        try:
            interval = int(task.get("INTERVAL_MIN"))
        except (TypeError, ValueError):
            return False

        if schedule_mode == "FIXED":
            ws = _parse_hhmm(task.get("WINDOW_START"))
            if not ws:
                return False
            base_min = ws[0] * 60 + ws[1]
            now_min  = now.hour * 60 + now.minute
            elapsed  = now_min - base_min
            if elapsed < 0:
                return False
            if (elapsed % interval) != 0:
                return False
            if last_run_str:
                try:
                    last = datetime.strptime(last_run_str, "%d-%m-%Y %H:%M:%S")
                    if last.hour == now.hour and last.minute == now.minute and last.date() == now.date():
                        return False
                except ValueError:
                    pass
            return True

        elif schedule_mode == "FIXED_TIMES":
            times_str = str(task.get("SCHEDULE_TIME") or "").strip()
            if not times_str:
                return False
            try:
                times_hm = [_parse_hhmm(t.strip()) for t in times_str.split(",")]
                times_hm = [x for x in times_hm if x]
            except Exception:
                return False
            if not times_hm:
                return False
            now_hm = (now.hour, now.minute)
            if now_hm not in times_hm:
                return False
            if last_run_str:
                try:
                    last = datetime.strptime(last_run_str, "%d-%m-%Y %H:%M:%S")
                    if last.date() == now.date() and last.hour == now.hour and last.minute == now.minute:
                        return False
                except ValueError:
                    pass
            return True

        else:
            if not last_run_str:
                log(f"[DUE?] {name} → DUE  ROLLING never run before")
                return True
            try:
                last = datetime.strptime(last_run_str, "%d-%m-%Y %H:%M:%S")
            except ValueError:
                return True
            elapsed_sec = (now - last).total_seconds()
            due         = elapsed_sec >= interval * 60
            return due

    return False

# PURPOSE: Builds a unique-ish identifier for one task execution.
def make_run_key(task_id, trigger: str) -> str:
    return f"{task_id}_{datetime.now():%Y%m%d_%H%M%S}_{trigger[:3]}"

# ─── Task execution ────────────────────────────────────────────────────────────

# PURPOSE: Starts the configured Python script, batch file or executable and registers monitoring.
def _launch(task: dict, trigger: str, slot_key: str = None, skip_watch: bool = False):
    path = str(task.get("SCRIPT_PATH", "")).strip()
    ext  = os.path.splitext(path)[1].lower() if path else ""

    if not path:
        log(f"[{trigger}] FILE_NOT_FOUND: path is empty")
        run_key = make_run_key(task["TASK_ID"], trigger)
        executor.submit(_upsert_status_row, run_key, task, "FILE_NOT_FOUND",
                        "SCRIPT_PATH is empty", trigger=trigger)
        return

    if ext == ".py" and not os.path.exists(path):
        log(f"[{trigger}] FILE_NOT_FOUND: {path}")
        run_key = make_run_key(task["TASK_ID"], trigger)
        executor.submit(_upsert_status_row, run_key, task, "FILE_NOT_FOUND",
                        f"Path not found: {path}", trigger=trigger)
        return

    try:
        run_key    = make_run_key(task["TASK_ID"], trigger)
        start_time = datetime.now()

        if ext == ".py":
            p = subprocess.Popen(["python", path], creationflags=subprocess.CREATE_NO_WINDOW)
        elif ext in (".bat", ".cmd"):
            # ✅ FIX: .bat/.cmd files need:
            # 1. cmd /c → waits for batch to finish
            # 2. cwd → set working directory to bat file's folder
            #    (bat files often use relative paths, so they fail if cwd is wrong)
            # 3. Redirect stderr to a log file for debugging
            bat_dir = os.path.dirname(os.path.abspath(path))
            err_log = os.path.join(LOG_DIR, f"bat_stderr_{task.get('TASK_NAME','unknown')}_{datetime.now():%Y%m%d_%H%M%S}.log")
            try:
                err_fh = open(err_log, "w")
            except Exception:
                err_fh = subprocess.DEVNULL
            p = subprocess.Popen(
                ["cmd", "/c", path],
                cwd=bat_dir,
                stdout=subprocess.DEVNULL,
                stderr=err_fh,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
        else:
            p = subprocess.Popen(
                path, shell=False,
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
            )

        with processes_lock:
            running_processes[run_key] = {
                "process"   : p,
                "task"      : task,
                "start_time": start_time,
                "trigger"   : trigger,
                "run_key"   : run_key,
            }

        next_dt  = calc_next_run(task, after=start_time)
        next_str = next_dt.strftime("%d-%m-%Y %H:%M:%S") if next_dt else "MANUAL"
        log(f"[LAUNCH] {task['TASK_NAME']} spawned (PID {p.pid}, {trigger})")
        executor.submit(_upsert_status_row, run_key, task, "RUNNING", "Executing",
                        pid=p.pid, next_run=next_str, trigger=trigger)

        effective_slot_key = slot_key or f"{task['TASK_ID']}_manual_{run_key}"

        if skip_watch:
            log(f"[LAUNCH] {task['TASK_NAME']} — watch skipped (caller managing retry watch)")
        else:
            log_watch_thread = threading.Thread(
                target=_watch_log_after_spawn,
                args=(task, start_time),
                kwargs={"slot_key": effective_slot_key},
                daemon=True,
                name=f"LogWatch-{run_key}"
            )
            log_watch_thread.start()
            log(f"[LOG_WATCH] {task['TASK_NAME']} — watch thread started (run_key={run_key})")

    except Exception as e:
        log(f"[LAUNCH ERROR] {task.get('TASK_NAME')}: {e}")
        run_key = make_run_key(task["TASK_ID"], trigger)
        executor.submit(_upsert_status_row, run_key, task, "FAILED", str(e), trigger=trigger)

# PURPOSE: Launches a task due to a manual request.
def execute_task_manual(task):
    task_id = str(task["TASK_ID"])
    with processes_lock:
        already = any(str(v["task"]["TASK_ID"]) == task_id for v in running_processes.values())
    if already:
        log(f"[MANUAL] BLOCKED — {task['TASK_NAME']} is already running.")
        return
    log(f"[MANUAL] Starting {task['TASK_NAME']}")
    _launch(task, "MANUAL")

# PURPOSE: Launches a task through the restart/recovery path.
def execute_task_restart(task):
    task_id = str(task["TASK_ID"])
    with processes_lock:
        targets = {rk: info for rk, info in running_processes.items()
                   if str(info["task"]["TASK_ID"]) == task_id}
    if targets:
        log(f"[E_RUN] {task['TASK_NAME']} is running — killing before restart")
        for rk, info in targets.items():
            p = info["process"]
            try:
                parent = psutil.Process(p.pid)
                for child in parent.children(recursive=True):
                    child.kill()
                parent.kill()
                log(f"[E_RUN] Killed {task['TASK_NAME']} run_key={rk} PID={p.pid}")
            except psutil.NoSuchProcess:
                pass
            except Exception as e:
                log(f"[E_RUN] Kill error: {e}")
            runtime = (datetime.now() - info["start_time"]).total_seconds()
            with processes_lock:
                running_processes.pop(rk, None)
            _upsert_status_row(rk, task, "KILLED", "Killed for restart (E_RUN)", pid=p.pid, duration=runtime)
        time.sleep(2)
    else:
        log(f"[E_RUN] {task['TASK_NAME']} not running — starting fresh")
    log(f"[E_RUN] Starting {task['TASK_NAME']}")
    _launch(task, "MANUAL")

# PURPOSE: Launches a task triggered by the scheduler.
def execute_task_scheduled(task, slot_key: str = None):
    log(f"[SCHEDULE] Starting {task['TASK_NAME']} (parallel allowed)")
    _launch(task, "SCHEDULED", slot_key=slot_key)

# PURPOSE: Terminates the tracked process for a task when permitted.
def kill_task(task):
    task_id = str(task["TASK_ID"])
    with processes_lock:
        targets = {rk: info for rk, info in running_processes.items()
                   if str(info["task"]["TASK_ID"]) == task_id}
    if not targets:
        log(f"[KILL] {task['TASK_NAME']} not running.")
        return
    for rk, info in targets.items():
        p = info["process"]
        try:
            parent = psutil.Process(p.pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
            log(f"[KILL] Killed {task['TASK_NAME']} run_key={rk} PID={p.pid}")
        except psutil.NoSuchProcess:
            pass
        except Exception as e:
            log(f"[KILL ERROR] {e}")
        runtime = (datetime.now() - info["start_time"]).total_seconds()
        with processes_lock:
            running_processes.pop(rk, None)
        _upsert_status_row(rk, task, "KILLED", "Killed by operator", pid=p.pid, duration=runtime)

# ─── Timeout ───────────────────────────────────────────────────────────────────

# PURPOSE: Sends an alert when a task exceeds its allowed runtime.
def send_timeout_mail(task, runtime_sec: float):
    recipients = str(task.get("ALERT_MAIL") or "").strip()
    if not recipients:
        return
    task_name   = task.get("TASK_NAME", "?")
    max_min     = task.get("MAX_RUNTIME_MIN", "?")
    elapsed_min = runtime_sec / 60
    host        = socket.gethostname()
    now_str     = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    subject = f"⏱ TIMEOUT — {task_name} exceeded {max_min} min limit"
    body = f"""<html><body style="font-family:Calibri,Arial;font-size:14px;color:#222;">
<h2 style="color:#c0392b;">⏱ Task Timeout Alert</h2>
<hr style="border:1px solid #e74c3c;">
<table cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:500px;">
  <tr style="background:#f9f9f9;"><td style="font-weight:bold;border:1px solid #ddd;width:160px;">Task Name</td><td style="border:1px solid #ddd;">{task_name}</td></tr>
  <tr><td style="font-weight:bold;border:1px solid #ddd;">Status</td><td style="border:1px solid #ddd;color:#c0392b;font-weight:bold;">KILLED (Timeout)</td></tr>
  <tr style="background:#f9f9f9;"><td style="font-weight:bold;border:1px solid #ddd;">Max Allowed</td><td style="border:1px solid #ddd;">{max_min} minutes</td></tr>
  <tr><td style="font-weight:bold;border:1px solid #ddd;">Actual Runtime</td><td style="border:1px solid #ddd;color:#e67e22;font-weight:bold;">{elapsed_min:.1f} minutes</td></tr>
  <tr style="background:#f9f9f9;"><td style="font-weight:bold;border:1px solid #ddd;">Killed At</td><td style="border:1px solid #ddd;">{now_str}</td></tr>
  <tr><td style="font-weight:bold;border:1px solid #ddd;">Host</td><td style="border:1px solid #ddd;">{host}</td></tr>
</table>
<p style="color:#7f8c8d;font-size:12px;">Automated alert from Task Scheduler.</p>
</body></html>"""
    try:
        outlook = _win32.Dispatch("Outlook.Application")
        mail    = outlook.CreateItem(0)
        for addr in recipients.replace(",", ";").split(";"):
            addr = addr.strip()
            if addr:
                mail.Recipients.Add(addr)
        mail.Subject  = subject
        mail.HTMLBody = body
        mail.Send()
        log(f"[MAIL] Timeout alert sent → {recipients}")
    except Exception as e:
        log(f"[MAIL ERROR] {task_name}: {e}")

# PURPOSE: Checks runtime and terminates a task that exceeds its configured timeout.
def _timeout_kill(run_key: str, task: dict, p, start_time: datetime):
    try:
        parent = psutil.Process(p.pid)
        for child in parent.children(recursive=True):
            child.kill()
        parent.kill()
    except psutil.NoSuchProcess:
        pass
    except Exception as e:
        log(f"[TIMEOUT KILL ERROR] {e}")
    runtime  = (datetime.now() - start_time).total_seconds()
    max_min  = task.get("MAX_RUNTIME_MIN", "?")
    next_dt  = calc_next_run(task, after=datetime.now())
    next_str = next_dt.strftime("%d-%m-%Y %H:%M:%S") if next_dt else "MANUAL"
    with processes_lock:
        running_processes.pop(run_key, None)
    _merge_parallel_rows(task, run_key, "TIMEOUT",
                         f"Killed — exceeded {max_min} min", p.pid, runtime, next_str)
    send_timeout_mail(task, runtime)

# ─── Monitor loop ──────────────────────────────────────────────────────────────

# PURPOSE: Periodically checks tracked processes and updates completion status.
def monitor_loop():
    while APP_RUNNING:
        try:
            with processes_lock:
                snapshot = list(running_processes.items())
            for run_key, info in snapshot:
                p          = info["process"]
                task       = info["task"]
                start_time = info["start_time"]
                rc         = p.poll()
                if rc is None:
                    max_min = task.get("MAX_RUNTIME_MIN")
                    try:
                        max_min = int(max_min)
                    except (TypeError, ValueError):
                        max_min = 0
                    if max_min and max_min > 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        if elapsed >= max_min * 60:
                            log(f"[TIMEOUT] {task['TASK_NAME']} run_key={run_key} exceeded {max_min} min — killing.")
                            # ✅ FIX: Use _launch_executor not executor
                            # executor (DB threads) may be full/blocked → timeout kill never runs
                            # _launch_executor is dedicated, always has free threads
                            _launch_executor.submit(_timeout_kill, run_key, task, p, start_time)
                    continue
                runtime  = (datetime.now() - start_time).total_seconds()
                next_dt  = calc_next_run(task, after=datetime.now())
                next_str = next_dt.strftime("%d-%m-%Y %H:%M:%S") if next_dt else "MANUAL"
                with processes_lock:
                    running_processes.pop(run_key, None)
                final_status = "SUCCESS" if rc == 0 else "CRASHED"
                final_msg    = "Completed" if rc == 0 else f"Exit code {rc}"
                _merge_parallel_rows(task, run_key, final_status, final_msg, p.pid, runtime, next_str)
        except Exception as e:
            log(f"[MONITOR ERROR] {e}")
        time.sleep(15)

# PURPOSE: Refreshes the scheduler heartbeat periodically.
def heartbeat_loop():
    """Heartbeat: writes to file every 30s, logs to console every 5 minutes."""
    log_counter = 0
    while APP_RUNNING:
        try:
            try:
                update_heartbeat()  # writes heartbeat file every 30s
            except Exception as hb_err:
                log(f"[HEARTBEAT] update_heartbeat error: {hb_err}")
            
            # Only log to file every 5 minutes (10 * 30s)
            log_counter += 1
            if log_counter >= 10:
                log_counter = 0
                with processes_lock:
                    running_count = len(running_processes)
                with _cache_lock:
                    task_count = len(_cached_tasks)
                log(f"[HEARTBEAT] Scheduler alive — {task_count} task(s), {running_count} running")
        except Exception as e:
            log(f"[HEARTBEAT ERROR] {e}")
        time.sleep(30)

# ─── Cache ─────────────────────────────────────────────────────────────────────

_cached_tasks:      list = []
_cached_status_map: dict = {}
_cache_lock              = threading.Lock()
_fired_slots:       set  = set()
_fired_lock              = threading.Lock()
_escalated_slots:   set  = set()
_escalation_lock         = threading.Lock()
_agent_enabled: bool     = False
_agent_lock              = threading.Lock()
_outbox_mode: str        = "MANUAL"
_outbox_mode_lock        = threading.Lock()
_outbox_last_check       = 0.0

# (schedule-change detection moved to _get_live_fixed_schedule / is_due_fixed_live)

# PURPOSE: Refreshes cached task configuration from SQLite and detects configuration changes.
def _refresh_cache():
    global _cached_tasks, _cached_status_map, _agent_enabled

    while APP_RUNNING:
        try:
            tasks = load_tasks()
            
            # ✅ Successfully read from SQLite → refresh compatibility backup table (silent, no log)
            _save_task_master_backup(tasks)

            def _rs(conn):
                rows = conn.execute(
                    "SELECT TASK_ID, LAST_RUN, UPDATED_TIME FROM TASK_STATUS ORDER BY UPDATED_TIME DESC"
                ).fetchall()
                seen, result = set(), {}
                for row in rows:
                    task_id, last_run, updated_time = row[0], row[1], row[2]
                    if task_id not in seen:
                        result[task_id] = (last_run or "")
                        seen.add(task_id)
                return result

            status_map = _sqlite_run(_rs)

            with _cache_lock:
                _cached_tasks      = tasks
                _cached_status_map = status_map

            enabled = is_agent_enabled()
            with _agent_lock:
                prev           = _agent_enabled
                _agent_enabled = enabled
            if enabled != prev:
                log(f"[AGENT_MODE] {'ENABLED — NEXA email watching ON' if enabled else 'DISABLED — NEXA email watching OFF'}")

            row = _read_agent_mode_row()
            new_mode = row.get("OUTBOX_CHECK", "MANUAL").upper()
            with _outbox_mode_lock:
                global _outbox_mode
                _outbox_mode = new_mode

            # Silent refresh — only log if task count or mode changed
            pass
        except Exception as e:
            # ✅ FIX: Instant fallback — no 60s wait
            # SQLite task read failed
            # → Fall back to the last cached task definitions if available
            if not _cached_tasks:
                backup_tasks = _load_task_master_backup()
                if backup_tasks:
                    with _cache_lock:
                        _cached_tasks = backup_tasks
                    log(f"[CACHE] SQLite task table unavailable — loaded {len(backup_tasks)} task(s) from backup table")
                else:
                    log("[CACHE] ⚠️ SQLite unavailable AND no backup — scheduler has no tasks")
            # If _cached_tasks already has data, just keep using it (no log spam)

        time.sleep(30)

# ─── Log path helpers ──────────────────────────────────────────────────────────

# PURPOSE: Loads configured log paths and their last-known timestamps.
def load_log_paths() -> dict:
    def _fn(conn, cur):
        try:
            cur.execute("SELECT TASK_ID, LOG_PATH FROM LOG_PATHS")
            return {int(row[0]): str(row[1] or "").strip() for row in cur.fetchall() if row[1]}
        except Exception:
            return {}
    try:
        with db_lock:
            return _run_with_retry(_fn)
    except Exception:
        return {}

# PURPOSE: Persists log creation/update timestamps for a task.
def _write_log_times(task_id: int, created_time_str: str = None, log_updated_str: str = None):
    def _fn(conn, cur):
        try:
            cur.execute("SELECT LOG_UPDATED FROM LOG_PATHS LIMIT 1")
        except Exception:
            try:
                cur.execute("ALTER TABLE LOG_PATHS ADD COLUMN LOG_UPDATED TEXT")
            except Exception:
                pass
        if created_time_str is not None:
            cur.execute(
                "UPDATE LOG_PATHS SET Created_time=? WHERE TASK_ID=?",
                (created_time_str, int(task_id))
            )
        if log_updated_str is not None:
            cur.execute(
                "UPDATE LOG_PATHS SET LOG_UPDATED=? WHERE TASK_ID=?",
                (log_updated_str, int(task_id))
            )
    try:
        with db_lock:
            _run_with_retry(_fn)
    except Exception as e:
        log(f"[LOG_WATCH] Could not write log times: {e}")

# PURPOSE: Reads the stored log timestamps for a task.
def _read_log_times(task_id: int) -> tuple:
    def _fn(conn, cur):
        cur.execute("SELECT * FROM LOG_PATHS WHERE TASK_ID=?", (int(task_id),))
        row = cur.fetchone()
        if not row:
            return None, None, None
        col_map      = {c[0].upper(): i for i, c in enumerate(cur.description)}
        log_path     = str(row[col_map["LOG_PATH"]] or "").strip()     if "LOG_PATH"     in col_map else None
        created_time = str(row[col_map["CREATED_TIME"]] or "").strip() if "CREATED_TIME" in col_map else None
        log_updated  = str(row[col_map["LOG_UPDATED"]] or "").strip()  if "LOG_UPDATED"  in col_map else None
        return log_path or None, created_time or None, log_updated or None
    try:
        with db_lock:
            return _run_with_retry(_fn)
    except Exception as e:
        log(f"[LOG] _read_log_times error: {e}")
        return None, None, None

# PURPOSE: Finds the newest relevant modification time for a file or log directory.
def _get_log_mtime(log_path: str, min_mtime: float = None) -> float | None:
    """
    Detects log file activity on network shares.
    Uses os.stat on the FOLDER itself (fast, one syscall) instead of
    listing all files (slow, blocks on locked files).
    Windows updates a folder's mtime whenever a file inside it is created
    or modified — so checking the folder's mtime tells us if any log file
    activity happened, without needing to enumerate files.
    """
    try:
        if not os.path.exists(log_path):
            return None
        
        # Fast path: check folder or file mtime directly (one os.stat call)
        # This is much faster than os.listdir() on network shares
        try:
            mtime = os.stat(log_path).st_mtime
            if min_mtime is None or mtime >= min_mtime:
                return mtime
        except Exception:
            pass
        
        return None
    except Exception:
        return None

# PURPOSE: Sends an email alert when expected log activity is not detected.
def _send_log_alert_mail(task: dict, log_path: str, last_mtime_str: str):
    recipients = str(task.get("ALERT_MAIL") or "").strip()
    if not recipients:
        log(f"[LOG_ALERT] No ALERT_MAIL set for {task.get('TASK_NAME')} — skipping mail")
        return
    task_name = task.get("TASK_NAME", "?")
    host      = socket.gethostname()
    now_str   = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    subject = f"⚠️ LOG NOT UPDATED — {task_name} may have failed"
    body = f"""<html><body style="font-family:Calibri,Arial;font-size:14px;color:#222;">
<h2 style="color:#e67e22;">⚠️ Task Log Not Updated</h2>
<hr style="border:1px solid #e67e22;">
<table cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:520px;">
  <tr style="background:#fff8f0;"><td style="font-weight:bold;border:1px solid #ddd;width:160px;">Task Name</td><td style="border:1px solid #ddd;">{task_name}</td></tr>
  <tr><td style="font-weight:bold;border:1px solid #ddd;">Log Path</td><td style="border:1px solid #ddd;font-size:12px;">{log_path}</td></tr>
  <tr style="background:#fff8f0;"><td style="font-weight:bold;border:1px solid #ddd;">Last Modified</td><td style="border:1px solid #ddd;color:#e74c3c;">{last_mtime_str}</td></tr>
  <tr><td style="font-weight:bold;border:1px solid #ddd;">Alert Time</td><td style="border:1px solid #ddd;">{now_str}</td></tr>
  <tr style="background:#fff8f0;"><td style="font-weight:bold;border:1px solid #ddd;">Host</td><td style="border:1px solid #ddd;">{host}</td></tr>
  <tr><td style="font-weight:bold;border:1px solid #ddd;">Action Taken</td><td style="border:1px solid #ddd;color:#c0392b;">Task was killed and retried once — log still not updated</td></tr>
</table>
<p style="color:#7f8c8d;font-size:12px;margin-top:12px;">Please investigate manually. This alert fires once per scheduled slot.</p>
</body></html>"""
    try:
        outlook = _win32.Dispatch("Outlook.Application")
        mail    = outlook.CreateItem(0)
        for addr in recipients.replace(",", ";").split(";"):
            addr = addr.strip()
            if addr:
                mail.Recipients.Add(addr)
        mail.Subject  = subject
        mail.HTMLBody = body
        mail.Send()
        log(f"[LOG_ALERT] Mail sent → {recipients}")
    except Exception as e:
        log(f"[LOG_ALERT] Mail failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# ✅ BUG #1 FIX — _watch_log_after_spawn
#
# ROOT CAUSE (confirmed from 01-08-2026 11:15 log):
#   Thread started at 11:15:51, logged "watching...for 5 min" then DIED SILENTLY.
#   No heartbeat at 11:16:51 confirms thread never entered the poll loop.
#   Thread died in setup code AFTER "watching..." log line — in _write_log_times().
#   Outer except caught the error, called log(), but log file was momentarily
#   locked by another thread → log() also failed → ZERO trace anywhere.
#
# FIXES:
#   1. print()+log() in ALL except blocks — print() survives log file contention
#   2. STEP1/STEP2 log before each DB call — pinpoints exactly where death occurred
#   3. _write_log_times failure is now WARNING (non-fatal) — thread continues
#   4. Per-run escalation guard (run_guard_key) — each run can escalate once
#   5. Poll heartbeat every 10 polls (~100s) — always visible in a 5-min window
# ═══════════════════════════════════════════════════════════════════════════════

# PURPOSE: Watches for log activity after a task starts and applies retry/window/escalation rules.
# This background watcher performs the actual repeated log polling after launch.
# LangGraph helps decide STOP/RETRY/ALERT; this function still owns the polling loop.
def _watch_log_after_spawn(task: dict, start_time: datetime, slot_key: str = None):
    task_id   = int(task.get("TASK_ID", 0))
    task_name = task.get("TASK_NAME", "?")
    log_check = str(task.get("LOG_CHECK", "AUTO")).upper().strip()

    try:
        watch_minutes = int(task.get("MINUTES", 5))
        if watch_minutes <= 0:
            watch_minutes = 5
    except (TypeError, ValueError):
        watch_minutes = 5

    WATCH_SEC  = watch_minutes * 60
    POLL_SEC   = 10
    RETRY_WAIT = 60


    if log_check == "MANUAL":
        log(f"[LOG_WATCH] {task_name} — LOG_CHECK=MANUAL, skipping")
        return

    if not slot_key:
        slot_key = f"{task_id}_{start_time:%Y%m%d_%H:%M}"

    # ✅ Per-run guard keyed to exact start timestamp — not the minute slot
    run_guard_key = f"esc_{task_id}_{start_time:%Y%m%d_%H%M%S}"

    # ── STEP 1: Load log paths ────────────────────────────────────────────────
    try:
        log_paths = load_log_paths()
    except Exception as e:
        print(f"[LOG_WATCH] {task_name} ❌ STEP1 FAILED — load_log_paths: {e}", flush=True)
        log(f"[LOG_WATCH] {task_name} ❌ STEP1 FAILED — load_log_paths: {e}")
        return

    if task_id not in log_paths:
        log(f"[LOG_WATCH] {task_name} — no LOG_PATH in LOG_PATHS table, skipping watch")
        return

    log_path = log_paths[task_id]
    log(f"[LOG_WATCH] {task_name} — STEP1 OK: log_path={log_path}")

    # ── STEP 2: Write created time (NON-FATAL) ────────────────────────────────
    try:
        _write_log_times(task_id, created_time_str=start_time.strftime("%d-%m-%Y %H:%M:%S"))
    except Exception as e:
        # ✅ WARNING only — a DB audit write failure must NOT kill the log watch
        print(f"[LOG_WATCH] {task_name} ⚠️ STEP2 WARNING — _write_log_times failed (continuing): {e}", flush=True)
        log(f"[LOG_WATCH] {task_name} ⚠️ STEP2 WARNING — _write_log_times failed (continuing): {e}")

    _ws = str(task.get("WINDOW_START") or "").strip()
    _we = str(task.get("WINDOW_END")   or "").strip()
    has_window = bool(_ws and _we)
    log(f"[LOG_WATCH] {task_name} — watching log path for {watch_minutes}min")

    # ─────────────────────────────────────────────────────────────────────────

    def _get_log_mtime_safe(log_path_arg: str, effective_since: float,
                             timeout_sec: float = 8.0):
        """
        BUG FIX: Run _get_log_mtime() in a background thread with a hard
        timeout. On Windows network shares (T:\\INTERNAL\\...), os.listdir()
        and os.stat() can BLOCK for the entire duration of a task run if the
        task holds a file lock on the folder. Without this wrapper, a single
        blocked os.stat() call freezes the poll loop for 40+ minutes while
        the waited counter stays stuck, making WATCH_SEC meaningless.
        Returns mtime float, or None if not found / timed out.
        """
        result_box = [None]
        error_box  = [None]

        def _worker():
            try:
                result_box[0] = _get_log_mtime(log_path_arg, min_mtime=effective_since)
            except Exception as e:
                error_box[0] = e

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        t.join(timeout=timeout_sec)

        if t.is_alive():
            log(f"[LOG_WATCH] {task_name} WARNING _get_log_mtime timed out after "
                f"{timeout_sec:.0f}s (file lock / network latency) — skipping poll")
            return None
        if error_box[0]:
            raise error_box[0]
        return result_box[0]

    def _read_folder_mtime():
        """Read folder/file mtime with no time filter — returns current mtime or None."""
        try:
            if not os.path.exists(log_path):
                return None
            return os.stat(log_path).st_mtime
        except Exception:
            return None

    def _poll_for_log(since_ts: float, label: str) -> bool:
        """
        CHANGE-DETECTION LOG WATCH:
        
        Instead of filtering by "log created after task start" (which fails when
        the log file was created by a previous run at an earlier time), we:
        
        1. At poll start: read BASELINE folder mtime
        2. Every 10 seconds: read CURRENT folder mtime
        3. If current != baseline → log was updated (task is writing) ✅
        
        This works even if the log file was created BEFORE the task started
        (previous run). Any WRITE to the log will change the folder mtime,
        which we detect as a change from baseline.
        """
        poll_start_ts = time.time()
        
        # Capture baseline mtime (what the folder looked like BEFORE polling)
        baseline_mtime = _read_folder_mtime()
        baseline_str = datetime.fromtimestamp(baseline_mtime).strftime("%H:%M:%S") if baseline_mtime else "none"
        log(f"[LOG_WATCH] {task_name} — baseline mtime: {baseline_str}")

        while (time.time() - poll_start_ts) < WATCH_SEC and APP_RUNNING:
            try:
                time.sleep(POLL_SEC)

                # Read current mtime
                current_mtime = _read_folder_mtime()
                
                if current_mtime is None:
                    continue  # can not read — try again next poll
                
                # CHANGE DETECTION: any change from baseline means log activity
                if baseline_mtime is None or current_mtime > baseline_mtime:
                    mtime_str = datetime.fromtimestamp(current_mtime).strftime("%d-%m-%Y %H:%M:%S")
                    log(f"[LOG_WATCH] {task_name} log activity detected — {mtime_str}")
                    try:
                        _write_log_times(task_id, log_updated_str=mtime_str)
                    except Exception:
                        pass
                    return True

            except Exception as e:
                log(f"[LOG_WATCH] {task_name} {label} poll exception: {e}")
                return False

        log(f"[LOG_WATCH] {task_name} — {label}: {WATCH_SEC}s elapsed, no log activity detected")
        return False

    def _is_window_closed() -> bool:
        return has_window and not in_window(task)

    def _kill_running_instances(label: str) -> dict:
        with processes_lock:
            targets = {rk: info for rk, info in running_processes.items()
                       if int(info["task"].get("TASK_ID", 0)) == task_id}

        still_alive = {}
        for rk, info in targets.items():
            rc = info["process"].poll()
            if rc is None:
                still_alive[rk] = info
            elif rc == 0:
                log(f"[LOG_WATCH] {task_name} — {rk} already exited SUCCESS (rc=0)")
            else:
                log(f"[LOG_WATCH] {task_name} — {rk} already exited rc={rc}")

        if not still_alive:
            log(f"[LOG_WATCH] {task_name} — no running processes to kill ({label})")
            return {}

        log(f"[LOG_WATCH] {task_name} ❌ no log after {label} — killing {len(still_alive)} process(es)")
        for rk, info in still_alive.items():
            try:
                parent = psutil.Process(info["process"].pid)
                for child in parent.children(recursive=True):
                    child.kill()
                parent.kill()
                log(f"[LOG_WATCH] {task_name} — killed PID {info['process'].pid}")
            except psutil.NoSuchProcess:
                pass
            except Exception as e:
                log(f"[LOG_WATCH] {task_name} — kill error: {e}")
        with processes_lock:
            for rk in still_alive:
                running_processes.pop(rk, None)
        return still_alive

    # ── MAIN ESCALATION LOGIC ─────────────────────────────────────────────────
    try:
        # ATTEMPT 1
        if _poll_for_log(start_time.timestamp(), "attempt-1"):
            return

        first_decision = _log_watch_decision(
            activity_found=False,
            window_open=not _is_window_closed(),
            attempt=1,
        )
        if first_decision != "retry":
            log(f"[LOG_WATCH] {task_name} ❌ no log after attempt-1, decision={first_decision} — stopping, no retry")
            return

        # ✅ Guard keyed to this run — each execution escalates at most once
        with _escalation_lock:
            if run_guard_key in _escalated_slots:
                log(f"[LOG_WATCH] {task_name} — escalation already done for this run, skipping")
                return
            _escalated_slots.add(run_guard_key)
            today = datetime.now().strftime("%Y%m%d")
            stale = {k for k in _escalated_slots if today not in k}
            _escalated_slots.difference_update(stale)

        killed = _kill_running_instances("attempt-1")
        if not killed:
            log(f"[LOG_WATCH] {task_name} — nothing was killed, no retry needed")
            return

        log(f"[LOG_WATCH] {task_name} — waiting {RETRY_WAIT}s before retry...")
        if _sleep_checked(RETRY_WAIT):
            log(f"[LOG_WATCH] {task_name} — shutdown during retry wait")
            return

        if _is_window_closed():
            log(f"[LOG_WATCH] {task_name} ❌ WINDOW CLOSED during wait — no retry")
            return

        # RETRY LAUNCH
        log(f"[LOG_WATCH] {task_name} — RETRY: relaunching task...")
        retry_time = datetime.now()
        try:
            _launch(task, "SCHEDULED", slot_key=slot_key, skip_watch=True)
        except Exception as e:
            print(f"[LOG_WATCH] {task_name} ❌ retry launch failed: {e}", flush=True)
            log(f"[LOG_WATCH] {task_name} ❌ retry launch failed: {e}")
            return

        # ATTEMPT 2
        if _poll_for_log(retry_time.timestamp(), "attempt-2"):
            return

        second_decision = _log_watch_decision(
            activity_found=False,
            window_open=not _is_window_closed(),
            attempt=2,
        )
        if second_decision != "alert":
            log(f"[LOG_WATCH] {task_name} ❌ no log after attempt-2, decision={second_decision} — no alert")
            return

        log(f"[LOG_WATCH] {task_name} ❌ no log after attempt-2 — killing retry process")
        _kill_running_instances("attempt-2")

        try:
            mtime    = _get_log_mtime(log_path)
            last_str = datetime.fromtimestamp(mtime).strftime("%d-%m-%Y %H:%M:%S") if mtime else "Never"
        except Exception:
            last_str = "Never"
        log(f"[LOG_WATCH] {task_name} ❌ sending alert mail (last log: {last_str})")
        try:
            _send_log_alert_mail(task, log_path, last_str)
        except Exception as e:
            log(f"[LOG_WATCH] {task_name} — alert mail failed: {e}")

    except Exception as e:
        # ✅ Full traceback via print() AND log() — survives log file lock
        tb = traceback.format_exc()
        print(f"[LOG_WATCH] {task_name} ❌ FATAL EXCEPTION:\n{tb}", flush=True)
        log(f"[LOG_WATCH] {task_name} ❌ FATAL EXCEPTION: {e}")
        log(f"[LOG_WATCH] {task_name} TRACEBACK:\n{tb}")


# PURPOSE: Runs periodic log-monitor checks outside the per-run watcher.
def log_monitor_loop():
    log("[LOG_MONITOR] Per-task log watch active")
    while APP_RUNNING:
        time.sleep(3600)


# ═══════════════════════════════════════════════════════════════════════════════
# ✅ BUG #2 FIX — _detect_and_clear_schedule_changes + scheduler_loop
#
# ROOT CAUSE:
#   INTERVAL+FIXED tasks (ID=1 KAM Report, ID=2 CC Level ECOM) use
#   WINDOW_START as their base for slot calculation. When WINDOW_START
#   changes in the DB, cache refreshes within 30s and is_due() recalculates
#   correctly, BUT _fired_slots still holds today's old time slots.
#   Clearing _fired_slots for the affected task ensures new schedule
#   takes effect within 1 second of the cache refresh.
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Live schedule reader for FIXED tasks ────────────────────────────────────
#
# ROOT CAUSE (confirmed):
#   _cached_tasks is refreshed every 30s. scheduler_loop reads from it every 1s.
#   If user changes WINDOW_START at 12:14:40 and cache last refreshed at 12:14:30,
#   the next cache refresh is at 12:15:00 — but scheduler_loop already ran at
#   12:15:00 with the OLD WINDOW_START=09:15, so is_due() said DUE and fired.
#
#   The ENTIRE approach of "detect change then clear slots" is wrong because
#   the change detection happens AFTER the bad fire already occurred.
#
# REAL FIX:
#   For INTERVAL+FIXED tasks, read WINDOW_START and INTERVAL_MIN DIRECTLY from
#   the DB at the moment is_due() is called — not from the 30s-stale cache.
#   This guarantees is_due() always uses the CURRENT value.
#
#   To avoid a DB hit every second per FIXED task, we use a 5-second micro-cache:
#   small dict {task_id: (ws, iv, fetched_at)} — re-queries DB only if data is
#   older than 5 seconds. This gives <5s lag on changes vs 30s before.
# ─────────────────────────────────────────────────────────────────────────────

_live_schedule_cache: dict = {}   # {task_id: {"WINDOW_START":..,"INTERVAL_MIN":..,"ts":float}}
_live_schedule_lock        = threading.Lock()
LIVE_SCHEDULE_TTL          = 5.0  # seconds — re-read DB if older than this

# PURPOSE: Reads the latest fixed schedule directly from SQLite instead of relying only on cache.
def _get_live_fixed_schedule(task_id: int) -> dict | None:
    """
    Returns {"WINDOW_START": "HH:MM", "INTERVAL_MIN": N} read live from DB,
    cached for LIVE_SCHEDULE_TTL seconds to avoid per-second DB hammering.
    Returns None on DB error (caller falls back to cached task dict).
    """
    now_ts = time.time()

    with _live_schedule_lock:
        cached = _live_schedule_cache.get(task_id)
        if cached and (now_ts - cached["ts"]) < LIVE_SCHEDULE_TTL:
            return cached

    def _fn(conn, cur):
        cur.execute(
            "SELECT WINDOW_START, INTERVAL_MIN FROM TASK_MASTER WHERE TASK_ID=?",
            (int(task_id),)
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "WINDOW_START": str(row[0] or "").strip(),
            "INTERVAL_MIN": row[1],
        }

    try:
        with db_lock:
            result = _run_with_retry(_fn)
        if result is None:
            return None
        result["ts"] = now_ts
        with _live_schedule_lock:
            old = _live_schedule_cache.get(task_id, {})
            _live_schedule_cache[task_id] = result
            # Log if WINDOW_START or INTERVAL_MIN changed
            if old and (old.get("WINDOW_START") != result["WINDOW_START"] or
                        str(old.get("INTERVAL_MIN")) != str(result["INTERVAL_MIN"])):
                log(f"[LIVE_SCHEDULE] Task ID={task_id}: "
                    f"WINDOW_START {old.get('WINDOW_START')}→{result['WINDOW_START']}  "
                    f"INTERVAL_MIN {old.get('INTERVAL_MIN')}→{result['INTERVAL_MIN']} "
                    f"(detected within {LIVE_SCHEDULE_TTL}s)")
                # Also clear any stale fired slots for this task
                today = datetime.now().strftime("%Y%m%d")
                with _fired_lock:
                    before = len(_fired_slots)
                    stale  = {k for k in _fired_slots
                              if k.startswith(f"{task_id}_") and today in k}
                    _fired_slots.difference_update(stale)
                    cleared = before - len(_fired_slots)
                    if cleared:
                        log(f"[LIVE_SCHEDULE] Task ID={task_id}: cleared {cleared} "
                            f"stale fired slot(s) — new schedule takes effect immediately")
        return result
    except Exception as e:
        log(f"[LIVE_SCHEDULE] DB read failed for task {task_id}: {e} — using cache")
        with _live_schedule_lock:
            return _live_schedule_cache.get(task_id)


# PURPOSE: Checks due status for a task using its live fixed schedule.
def is_due_fixed_live(task: dict, last_run_str: str, now: datetime) -> bool:
    """
    ✅ FIXED: Uses _cached_tasks data (refreshed every 30s in a separate thread).
    
    PREVIOUS BUG: Called _get_live_fixed_schedule() which reads from Access DB.
    If Access was locked, this BLOCKED the scheduler loop for up to 60 seconds,
    causing the entire minute slot to be missed. KAM 16:40, CC 17:35 etc.
    would never fire because by the time the lock released, the minute had passed.
    
    NOW: Uses task dict from _cached_tasks (refreshed every 30s by a separate
    non-blocking thread). 30s delay on WINDOW_START changes is acceptable.
    Missing hour-slots is NOT acceptable.
    """
    task_id   = int(task.get("TASK_ID", 0))
    task_name = task.get("TASK_NAME", f"ID={task_id}")

    # Use cached task data (never blocks)
    ws_str = str(task.get("WINDOW_START") or "").strip()
    iv_raw = task.get("INTERVAL_MIN")

    # Parse WINDOW_START
    try:
        ws_h, ws_m = map(int, ws_str.split(":"))
    except (ValueError, AttributeError):
        return False

    # Parse INTERVAL_MIN
    try:
        interval = int(iv_raw)
    except (TypeError, ValueError):
        return False

    # Window check
    if not in_window(task):
        return False

    base_min = ws_h * 60 + ws_m
    now_min  = now.hour * 60 + now.minute
    elapsed  = now_min - base_min

    if elapsed < 0:
        return False
    if (elapsed % interval) != 0:
        return False

    # Already ran this exact minute today?
    if last_run_str:
        try:
            last = datetime.strptime(last_run_str, "%d-%m-%Y %H:%M:%S")
            if last.date() == now.date() and last.hour == now.hour and last.minute == now.minute:
                return False
        except ValueError:
            pass

    return True



# PURPOSE: Persists a schedule-slot key so the same slot is not launched twice.
def _persist_fired_slot(slot_key: str):
    """Save a fired slot to SQLite so it survives Python restarts."""
    def _fn(conn):
        conn.execute(
            "INSERT OR IGNORE INTO FIRED_SLOTS (slot_key, fired_at) VALUES (?, ?)",
            (slot_key, datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
        )
        # Keep only this date's slots. The date is embedded as YYYYMMDD in slot_key.
        # This avoids unreliable lexical comparisons of DD-MM-YYYY timestamps.
        today_key = datetime.now().strftime("%Y%m%d")
        conn.execute(
            "DELETE FROM FIRED_SLOTS WHERE instr(slot_key, '_' || ? || '_') = 0",
            (today_key,)
        )
    try:
        _sqlite_run(_fn)
    except Exception:
        pass  # Non-critical — in-memory set still works


# PURPOSE: Loads today's already-fired slot keys into memory after startup.
def _restore_fired_slots() -> set:
    """Restore today's fired slots from SQLite on startup."""
    def _fn(conn):
        today = datetime.now().strftime("%Y%m%d")
        rows = conn.execute(
            "SELECT slot_key FROM FIRED_SLOTS WHERE instr(slot_key, '_' || ? || '_') > 0",
            (today,)
        ).fetchall()
        return {row[0] for row in rows}
    try:
        return _sqlite_run(_fn)
    except Exception:
        return set()

# PURPOSE: Typed state dictionary passed between the main LangGraph scheduler nodes.
# Main graph state: nodes exchange these values without using an LLM.
class SchedulerGraphState(TypedDict, total=False):
    """State passed between deterministic LangGraph scheduler nodes."""
    now: datetime
    tasks: list[dict]
    status_map: dict
    due_tasks: list[tuple[dict, str]]
    launched_count: int
    error_count: int


# PURPOSE: Typed state dictionary passed between the log-monitor decision nodes.
# Log-watch graph state: only the values needed to choose a deterministic action.
class LogWatchDecisionState(TypedDict, total=False):
    """State for deterministic log-watch escalation decisions; no LLM."""
    activity_found: bool
    window_open: bool
    attempt: int
    action: str


# PURPOSE: LangGraph decision node: chooses stop, retry or alert using Python conditions only.
# This node is deterministic: it reads state fields and returns an action string.
# No model is called; the same inputs produce the same decision.
def _graph_decide_log_watch(state: LogWatchDecisionState) -> LogWatchDecisionState:
    """Rule node: preserve existing behavior for log activity, retry, and alert."""
    if state.get("activity_found", False):
        action = "done"
    elif not state.get("window_open", False):
        action = "stop"
    elif int(state.get("attempt", 1)) == 1:
        action = "retry"
    else:
        action = "alert"
    log(
        "[LANGGRAPH LOG DECISION] "
        f"attempt={state.get('attempt', 1)} "
        f"activity={bool(state.get('activity_found', False))} "
        f"window_open={bool(state.get('window_open', False))} -> {action}"
    )
    return {"action": action}


# PURPOSE: Builds and compiles the small rule-based graph for log-watch decisions.
def _build_log_watch_decision_graph():
    """Small rule-only graph used by the background log watcher at decision points."""
    graph = StateGraph(LogWatchDecisionState)
    graph.add_node("decide_log_outcome", _graph_decide_log_watch)
    graph.set_entry_point("decide_log_outcome")
    graph.add_edge("decide_log_outcome", END)
    return graph.compile()


_LOG_WATCH_DECISION_GRAPH = _build_log_watch_decision_graph()


# PURPOSE: Invokes the log decision graph and returns its selected action.
def _log_watch_decision(activity_found: bool, window_open: bool, attempt: int) -> str:
    """Route a log-watch result through LangGraph; deterministic and model-free."""
    result = _LOG_WATCH_DECISION_GRAPH.invoke({
        "activity_found": activity_found,
        "window_open": window_open,
        "attempt": attempt,
    })
    return str(result.get("action", "stop"))


# PURPOSE: Loads current time, task cache and fired-slot state into LangGraph state.
def _graph_load_scheduler_state(state: SchedulerGraphState) -> SchedulerGraphState:
    """Node 1: snapshot the scheduler cache and current local time."""
    with _cache_lock:
        tasks = list(_cached_tasks)
        status_map = dict(_cached_status_map)
    return {
        "now": datetime.now(),
        "tasks": tasks,
        "status_map": status_map,
        "due_tasks": [],
        "launched_count": 0,
        "error_count": 0,
    }


# PURPOSE: Evaluates tasks against schedule rules and stores the due-task list in graph state.
def _graph_evaluate_due_tasks(state: SchedulerGraphState) -> SchedulerGraphState:
    """Node 2: apply fixed Python scheduling rules; no LLM/model is used."""
    now = state["now"]
    due_tasks: list[tuple[dict, str]] = []
    errors = 0

    for task in state.get("tasks", []):
        task_id = str(task["TASK_ID"])
        last_run = state.get("status_map", {}).get(int(task_id), "")
        stype = str(task.get("SCHEDULE_TYPE") or "").upper().strip()
        smode = str(task.get("SCHEDULE_MODE") or "").upper().strip()

        try:
            has_interval = bool(task.get("INTERVAL_MIN"))
            if stype == "INTERVAL" and smode == "FIXED" and has_interval:
                due = is_due_fixed_live(task, last_run, now)
            else:
                due = is_due(task, last_run, now=now)
            if not due:
                continue
        except Exception as exc:
            errors += 1
            log(f"[LANGGRAPH SCHEDULER] is_due check failed for {task.get('TASK_NAME')}: {exc}")
            continue

        slot_key = f"{task_id}_{now:%Y%m%d_%H:%M}"
        with _fired_lock:
            if slot_key in _fired_slots:
                continue
            # Reserve in memory before queueing so overlapping graph ticks cannot duplicate it.
            _fired_slots.add(slot_key)
        due_tasks.append((task, slot_key))

    return {"due_tasks": due_tasks, "error_count": errors}


# PURPOSE: Routes the graph to dispatch when tasks are due, otherwise to cleanup.
def _graph_route_after_evaluation(state: SchedulerGraphState) -> str:
    """Conditional edge: dispatch when due tasks exist, otherwise clean up."""
    return "dispatch" if state.get("due_tasks") else "cleanup"


# PURPOSE: Persists due slots and submits the eligible task launches.
# This node launches only tasks already marked due by the evaluation node.
# It should not independently reinterpret the schedule.
def _graph_dispatch_due_tasks(state: SchedulerGraphState) -> SchedulerGraphState:
    """Node 3: persist slot reservations and submit work asynchronously."""
    launched = 0
    for task, slot_key in state.get("due_tasks", []):
        try:
            _persist_fired_slot(slot_key)
            _launch_executor.submit(execute_task_scheduled, task, slot_key)
            launched += 1
        except Exception as exc:
            # Release the in-memory reservation if the dispatch itself failed.
            with _fired_lock:
                _fired_slots.discard(slot_key)
            log(f"[LANGGRAPH SCHEDULER] Dispatch failed for {task.get('TASK_NAME')}: {exc}")
    return {"launched_count": launched}


# PURPOSE: Removes stale fired-slot records according to the daily cleanup rule.
def _graph_cleanup_daily_slots(state: SchedulerGraphState) -> SchedulerGraphState:
    """Node 4: retain only today's fired slots in memory and SQLite."""
    now = state.get("now", datetime.now())
    today = now.strftime("%Y%m%d")
    with _fired_lock:
        stale = {key for key in _fired_slots if today not in key}
        for key in stale:
            _fired_slots.discard(key)

    # SQL-based cleanup. The date token is YYYYMMDD inside each slot key.
    try:
        def _cleanup(conn):
            conn.execute(
                "DELETE FROM FIRED_SLOTS WHERE instr(slot_key, '_' || ? || '_') = 0",
                (today,),
            )
        _sqlite_run(_cleanup)
    except Exception as exc:
        log(f"[LANGGRAPH SCHEDULER] SQL daily-slot cleanup warning: {exc}")
    return {}


# PURPOSE: Builds the main LangGraph graph; no model or LLM is involved.
def _build_rule_based_scheduler_graph():
    """Build the scheduler graph with Python-only decision nodes (no LLM)."""
    # StateGraph defines the workflow and the dictionary shared by its nodes.
    graph = StateGraph(SchedulerGraphState)

    # Register each Python function as a named workflow node.
    graph.add_node("load_state", _graph_load_scheduler_state)       # Load clock, tasks and fired slots.
    graph.add_node("evaluate_due", _graph_evaluate_due_tasks)        # Apply schedule rules.
    graph.add_node("dispatch", _graph_dispatch_due_tasks)            # Launch tasks that passed validation.
    graph.add_node("cleanup", _graph_cleanup_daily_slots)            # Remove stale daily slot records.

    # First node executed whenever the graph is invoked.
    graph.set_entry_point("load_state")

    # Normal path: load state, then evaluate which tasks are due.
    graph.add_edge("load_state", "evaluate_due")

    # After evaluation, call a Python routing function.
    # It returns "dispatch" when there are due tasks, otherwise "cleanup".
    graph.add_conditional_edges(
        "evaluate_due",
        _graph_route_after_evaluation,
        {"dispatch": "dispatch", "cleanup": "cleanup"},
    )

    # Once dispatch finishes, always run cleanup.
    graph.add_edge("dispatch", "cleanup")

    # Cleanup ends this graph execution. The outer scheduler loop invokes it again.
    graph.add_edge("cleanup", END)

    # Compile the graph definition so it can be invoked with graph.invoke(...).
    return graph.compile()


# Compile once at startup; this graph contains no ChatModel/LLM nodes.
_RULE_BASED_SCHEDULER_GRAPH = _build_rule_based_scheduler_graph()


# PURPOSE: Repeatedly invokes the graph on scheduler ticks.
# The scheduler loop is the clock/tick driver; LangGraph does not create a timer.
# Each iteration invokes the graph, then sleeps until the next polling tick.
def scheduler_loop():
    """Run the deterministic LangGraph scheduler once per second."""
    log("[LANGGRAPH] Rule-based scheduler graph started (LLM disabled).")
    log("[LANGGRAPH] Log-watch decision graph is active; background watcher retains timed polling.")
    while APP_RUNNING:
        try:
            _RULE_BASED_SCHEDULER_GRAPH.invoke({})
        except Exception as exc:
            log(f"[LANGGRAPH SCHEDULER ERROR] {type(exc).__name__}: {exc}")
        time.sleep(1)


# PURPOSE: Writes a compact status summary for diagnostics.
def _log_scheduler_status():
    """Silent — no periodic scheduler status spam. Cache refresh log already shows task count."""
    while APP_RUNNING:
        time.sleep(3600)

# PURPOSE: Reads pending manual task requests from the database.
def _load_manual_actions() -> list[dict]:
    def _fn(conn, cur):
        cur.execute(
            "SELECT TASK_ID, MANUAL_ACTION FROM TASK_MASTER "
            "WHERE ENABLED='Y' AND MANUAL_ACTION IS NOT NULL"
        )
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    try:
        with db_lock:
            return _run_with_retry(_fn)
    except Exception:
        return []

# PURPOSE: Processes manual task requests independently of scheduled launches.
def manual_action_loop():
    while APP_RUNNING:
        try:
            pending = _load_manual_actions()
            for row in pending:
                action  = str(row.get("MANUAL_ACTION", "") or "").upper().strip()
                task_id = str(row["TASK_ID"])
                if not action:
                    continue
                with _cache_lock:
                    task = next((t for t in _cached_tasks
                                 if str(t.get("TASK_ID")) == task_id), None)
                if not task:
                    task = {"TASK_ID": int(task_id), "TASK_NAME": f"TASK_{task_id}"}
                    log(f"[MANUAL] Task {task_id} not in cache — using minimal dict")

                if action == "RUN":
                    _launch_executor.submit(execute_task_manual, task)
                    clear_manual_action(task_id)
                elif action == "KILL":
                    log(f"[MANUAL] KILL → {task.get('TASK_NAME', task_id)}")
                    _launch_executor.submit(kill_task, task)
                    clear_manual_action(task_id)
                elif action == "E_RUN":
                    log(f"[MANUAL] E_RUN → {task.get('TASK_NAME', task_id)} (kill + restart)")
                    _launch_executor.submit(execute_task_restart, task)
                    clear_manual_action(task_id)
        except Exception as e:
            log(f"[MANUAL ERROR] {e}")
        time.sleep(30)

# ─── Outbox Auto Monitor ───────────────────────────────────────────────────────

# PURPOSE: Deletes Outlook trash items according to the configured cleanup rules.
def _delete_trash_items() -> int:
    import pythoncom
    pythoncom.CoInitialize()
    try:
        outlook_ns = _win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
        trash      = outlook_ns.GetDefaultFolder(3)
        count      = trash.Items.Count
        if count == 0:
            return 0
        items     = trash.Items
        to_delete = []
        for i in range(1, count + 1):
            try:
                to_delete.append(items.Item(i))
            except Exception:
                pass
        deleted = 0
        for it in to_delete:
            try:
                it.Delete()
                deleted += 1
            except Exception:
                pass
        return deleted
    except Exception as e:
        log(f"[TRASH_DELETE] Error: {e}")
        return -1

# PURPOSE: Attempts to restart Outlook when the outbox workflow requires it.
def _force_restart_outlook():
    """
    Kill Outlook if running, wait for it to close, then relaunch it.
    Called when mail is stuck in outbox — a fresh Outlook clears send queue.
    """
    log("[OUTBOX] Force-restarting Outlook to clear stuck mail queue...")
    # Kill all Outlook instances
    killed = False
    for proc in psutil.process_iter(["name", "pid"]):
        try:
            if proc.info["name"] and proc.info["name"].lower() == "outlook.exe":
                proc.kill()
                killed = True
                log(f"[OUTBOX] Killed Outlook PID {proc.info['pid']}")
        except Exception:
            pass
    if killed:
        # Wait up to 15s for Outlook to fully exit
        for _ in range(15):
            time.sleep(1)
            if not any(p.name().lower() == "outlook.exe"
                       for p in psutil.process_iter(["name"])):
                break
        log("[OUTBOX] Outlook closed — relaunching...")
    # Relaunch Outlook (opens fresh, clears stuck queue)
    _ensure_outlook_running()


# PURPOSE: Runs the deterministic Outlook outbox checking and recovery rules.
def _outbox_auto_check():
    import pythoncom
    pythoncom.CoInitialize()

    # ✅ BUG FIX: Ensure Outlook is open before checking outbox.
    # If Outlook was accidentally closed, stuck mail can never be sent.
    # Open it now so the outbox check actually works.
    if not _ensure_outlook_running():
        log("[OUTBOX_AUTO] Outlook could not be opened — skipping outbox check")
        return

    try:
        outlook_app = _win32.Dispatch("Outlook.Application")
        ns          = outlook_app.GetNamespace("MAPI")
        outbox      = ns.GetDefaultFolder(4)
        sent        = ns.GetDefaultFolder(5)
        trash       = ns.GetDefaultFolder(3)
    except Exception as e:
        log(f"[OUTBOX_AUTO] Cannot connect to Outlook: {e}")
        return

    def _empty_trash():
        try:
            trash_count = trash.Items.Count
            if trash_count == 0:
                return 0
            items     = trash.Items
            to_delete = []
            for i in range(1, trash_count + 1):
                try:
                    to_delete.append(items.Item(i))
                except Exception:
                    pass
            deleted = 0
            for it in to_delete:
                try:
                    it.Delete()
                    deleted += 1
                except Exception:
                    pass
            if deleted > 0:
                log(f"[OUTBOX_AUTO] 🗑️ Emptied Trash — {deleted} item(s) deleted")
            return deleted
        except Exception as e:
            log(f"[OUTBOX_AUTO] Failed to empty trash: {e}")
            return 0

    count = outbox.Items.Count
    if count == 0:
        log("[OUTBOX_AUTO] Outbox empty — no action needed")
        return

    log(f"[OUTBOX_AUTO] {count} mail(s) stuck in outbox — waiting 5 min")
    if _sleep_checked(300):
        return

    count = outbox.Items.Count
    if count == 0:
        log("[OUTBOX_AUTO] Outbox cleared on its own")
        return

    log(f"[OUTBOX_AUTO] {count} mail(s) still stuck — starting delete cycle")
    max_cycles = 5
    for cycle in range(1, max_cycles + 1):
        sent_count = sent.Items.Count
        if sent_count == 0:
            log("[OUTBOX_AUTO] Sent items empty but outbox stuck — sending alert")
            row   = _read_agent_mode_row()
            email = row.get("EMAIL", "").strip()
            if email:
                try:
                    mail_alert          = outlook_app.CreateItem(0)
                    mail_alert.To       = email
                    mail_alert.Subject  = f"⚠️ OUTBOX STUCK — Sent items empty, cannot clear"
                    mail_alert.HTMLBody = f"""<html><body style="font-family:Calibri;font-size:14px;">
<h3 style="color:#e74c3c;">⚠️ Outbox Auto Monitor Alert</h3>
<p>There are <b>{outbox.Items.Count} mail(s)</b> stuck in the Outbox.</p>
<p>Attempted to clear by deleting sent items, but the <b>Sent Items folder is now empty</b>.</p>
<p>Please check Outlook manually.</p>
<p style="color:#999;font-size:12px;">{datetime.now().strftime("%d-%m-%Y %H:%M:%S")} | Host: {socket.gethostname()}</p>
</body></html>"""
                    mail_alert.Send()
                    log(f"[OUTBOX_AUTO] Alert sent to {email}")
                except Exception as e:
                    log(f"[OUTBOX_AUTO] Alert mail failed: {e}")
            return

        items     = sent.Items
        items.Sort("[SentOn]", False)
        delete_n  = min(10, sent_count)
        to_delete = []
        for i in range(1, delete_n + 1):
            try:
                to_delete.append(items.Item(i))
            except Exception:
                pass
        deleted = 0
        for m in to_delete:
            try:
                m.Delete()
                deleted += 1
            except Exception:
                pass
        log(f"[OUTBOX_AUTO] Cycle {cycle}: deleted {deleted} oldest sent item(s)")
        trash_deleted = _empty_trash()
        if trash_deleted > 0:
            log(f"[OUTBOX_AUTO] Cycle {cycle}: freed up space by emptying Trash")
        log(f"[OUTBOX_AUTO] Cycle {cycle}: waiting 5 min")
        if _sleep_checked(300):
            return
        count = outbox.Items.Count
        if count == 0:
            log(f"[OUTBOX_AUTO] Outbox cleared after cycle {cycle} ✅")
            return
        else:
            log(f"[OUTBOX_AUTO] {count} mail(s) still in outbox — continuing")

    log(f"[OUTBOX_AUTO] All {max_cycles} cycles failed — force-restarting Outlook as last resort")
    _force_restart_outlook()
    time.sleep(30)

    # Check if restart helped
    try:
        outlook_app2 = _win32.Dispatch("Outlook.Application")
        ns2          = outlook_app2.GetNamespace("MAPI")
        outbox2      = ns2.GetDefaultFolder(4)
        if outbox2.Items.Count == 0:
            log("[OUTBOX_AUTO] Outbox cleared after Outlook force-restart ✅")
            return
        log(f"[OUTBOX_AUTO] {outbox2.Items.Count} mail(s) still stuck after restart — sending alert")
    except Exception:
        pass

    log(f"[OUTBOX_AUTO] Max cycles reached — outbox still stuck, sending alert")
    row   = _read_agent_mode_row()
    email = row.get("EMAIL", "").strip()
    if email:
        try:
            mail_alert          = outlook_app.CreateItem(0)
            mail_alert.To       = email
            mail_alert.Subject  = f"⚠️ OUTBOX STUCK — Max retry cycles reached"
            mail_alert.HTMLBody = f"""<html><body style="font-family:Calibri;font-size:14px;">
<h3 style="color:#e74c3c;">⚠️ Outbox Auto Monitor Alert</h3>
<p><b>{outbox.Items.Count} mail(s)</b> remain stuck in Outbox after {max_cycles} delete cycles.</p>
<p>Please check Outlook manually.</p>
<p style="color:#999;font-size:12px;">{datetime.now().strftime("%d-%m-%Y %H:%M:%S")} | Host: {socket.gethostname()}</p>
</body></html>"""
            mail_alert.Send()
            log(f"[OUTBOX_AUTO] Alert sent to {email}")
        except Exception as e:
            log(f"[OUTBOX_AUTO] Alert mail failed: {e}")

# PURPOSE: Periodically runs the outbox checker when its configuration enables it.
def outbox_monitor_loop():
    log("[OUTBOX_MONITOR] Thread started")
    global _outbox_last_check
    while APP_RUNNING:
        try:
            with _outbox_mode_lock:
                mode = _outbox_mode
            if mode == "AUTO":
                now = time.time()
                if now - _outbox_last_check >= 3600:
                    log("[OUTBOX_AUTO] Starting hourly outbox check")
                    _outbox_last_check = now
                    executor.submit(_outbox_auto_check)
        except Exception as e:
            log(f"[OUTBOX_MONITOR ERROR] {e}")
        time.sleep(60)

# PURPOSE: Runs the Outlook trash-cleaning operation.
def _empty_outlook_trash():
    deleted = _delete_trash_items()
    if deleted >= 0:
        log(f"[TRASH_CLEANER] Emptied Deleted Items — {deleted} item(s) removed")
    else:
        log("[TRASH_CLEANER] Failed to empty trash")

# PURPOSE: Periodically triggers the trash-cleaning operation.
def trash_cleaner_loop():
    log("[TRASH_CLEANER] Thread started — scheduled for 22:00 daily")
    while APP_RUNNING:
        try:
            now    = datetime.now()
            target = now.replace(hour=22, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            sleep_sec = (target - now).total_seconds()
            log(f"[TRASH_CLEANER] Next run at {target.strftime('%d-%m-%Y %H:%M:%S')} "
                f"(sleeping {sleep_sec/3600:.1f}h)")
            if _sleep_checked(sleep_sec):
                log("[TRASH_CLEANER] Shutdown during sleep")
                break
            if not APP_RUNNING:
                break
            log("[TRASH_CLEANER] 22:00 reached — emptying trash")
            executor.submit(_empty_outlook_trash)
            time.sleep(60)
        except Exception as e:
            log(f"[TRASH_CLEANER ERROR] {e}")
            time.sleep(300)

# ─── NEXA Email Agent ──────────────────────────────────────────────────────────

# PURPOSE: Parses a NEXA email subject into a recognized command and parameters.
def _parse_nexa_subject(subject: str) -> tuple | None:
    subject = str(subject or "").strip()

    # NEXA:KEEP_INBOX_N_DAYS — dynamic N (e.g. NEXA:KEEP_INBOX_8_DAYS, NEXA:KEEP_INBOX_30_DAYS)
    m = re.search(r"NEXA:\s*KEEP[_\s]*INBOX[_\s]*(\d+)[_\s]*DAYS", subject, re.IGNORECASE)
    if m:
        return (-1, f"KEEP_INBOX_{m.group(1)}_DAYS")
    if re.search(r"NEXA:\s*TRASH[_\s]*DELETE", subject, re.IGNORECASE):
        return (-1, "TRASH_DELETE")
    if re.search(r"NEXA:\s*OUTBOX[_\s]*AUTO", subject, re.IGNORECASE):
        return (-1, "OUTBOX_AUTO")
    if re.search(r"NEXA:\s*OUTBOX[_\s]*MANUAL", subject, re.IGNORECASE):
        return (-1, "OUTBOX_MANUAL")
    if re.search(r"NEXA:\s*CHECK\s+OUTBOX", subject, re.IGNORECASE):
        return (-1, "CHECK_OUTBOX")
    if re.search(r"NEXA:\s*CHECK\s+OUTLOOK", subject, re.IGNORECASE):
        return (-1, "CHECK_OUTLOOK")

    m = re.search(r"NEXA:\s*KEEP[_\s]+(\d+)\s+MAIL\s+OUTBOX", subject, re.IGNORECASE)
    if m:
        return (int(m.group(1)), "KEEP_OUTBOX")
    m = re.search(r"NEXA:\s*DELETE[_\s]+(\d+)[_\s-]+INBOX", subject, re.IGNORECASE)
    if m:
        return (int(m.group(1)), "DELETE_INBOX")
    m = re.search(r"NEXA:\s*DELETE[_\s]+(\d+)[_\s-]+SENT", subject, re.IGNORECASE)
    if m:
        return (int(m.group(1)), "DELETE_SENT")
    if re.search(r"NEXA:OUTLOOK", subject, re.IGNORECASE):
        return (-1, "CHECK_OUTLOOK")

    match = re.search(
        r"NEXA:TASK[_\s]+(\d+)\s*-\s*(E_RUN|RUN|KILL|STATUS|ENABLE|DISABLE|LOG|SEND_LOG_FILE)",
        subject, re.IGNORECASE
    )
    if not match:
        return None
    return (int(match.group(1)), match.group(2).upper())

# PURPOSE: Sends a formatted response email for a processed NEXA request.
def _nexa_send_reply(sender_email: str, sender_name: str, original_subject: str,
                     action: str, task_name: str, detail_rows: list,
                     attachment_path: str = None):
    # Extract proper name from sender_name
    # If sender_name is "Kaaleedass.Manoharan@ril.com" → "Kaaleedass Manoharan"
    # If sender_name is "Kaaleedass Manoharan" → "Kaaleedass Manoharan"
    name_for_greeting = str(sender_name or "Unknown").strip()
    if "@" in name_for_greeting:
        # Email format: extract part before @ and replace dots with spaces
        name_for_greeting = name_for_greeting.split("@")[0].replace(".", " ").title()
    else:
        # Already a name, just use first part
        name_for_greeting = name_for_greeting.split()[0] if name_for_greeting else "Sir/Madam"
    
    first_name   = name_for_greeting
    action_upper = str(action).upper()
    badge_map = {
        "RUN":           ("#27ae60", "Task Triggered",    "▶"),
        "E_RUN":         ("#8e44ad", "Task Restarted",    "🔄"),
        "KILL":          ("#e74c3c", "Task Stopped",      "■"),
        "STATUS":        ("#2980b9", "Task Status",       "ℹ"),
        "ENABLE":        ("#27ae60", "Task Enabled",      "✅"),
        "DISABLE":       ("#e67e22", "Task Disabled",     "⛔"),
        "OUTLOOK":       ("#8e44ad", "Outlook Status",    "📧"),
        "LOG":           ("#0f6e56", "Log Status",        "📄"),
        "SEND_LOG_FILE": ("#0f6e56", "Log File Attached", "📎"),
        "TRASH_DELETE":  ("#c0392b", "Trash Emptied",     "🗑️"),
    }
    badge_color, badge_text, icon = badge_map.get(action_upper, ("#7f8c8d", "Action Completed", "•"))

    table_rows_html = ""
    for i, (label, value) in enumerate(detail_rows):
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        table_rows_html += f"""
        <tr style="background:{bg};">
          <td style="font-weight:bold;border:1px solid #ddd;padding:7px 12px;width:160px;color:#555;">{label}</td>
          <td style="border:1px solid #ddd;padding:7px 12px;">{value}</td>
        </tr>"""

    closing_map = {
        "RUN":           "The task has been queued for execution. Please allow a few minutes for it to complete.",
        "KILL":          "The task process has been terminated. You may re-trigger it at any time.",
        "STATUS":        "If the task is currently running, the duration shown reflects the last completed run.",
        "ENABLE":        "The task has been enabled and will resume its scheduled execution within the next cycle.",
        "DISABLE":       "The task has been disabled and will not be triggered automatically until re-enabled.",
        "OUTLOOK":       "If there are emails stuck in your Outbox, please check your Outlook connection.",
        "LOG":           "The log time shown reflects when the task last created or modified a file in the configured log path.",
        "SEND_LOG_FILE": "The attached file is the most recently created or modified log file in the configured log folder.",
        "TRASH_DELETE":  "All items in the Deleted Items (Trash) folder have been permanently removed.",
    }
    closing = closing_map.get(action_upper, "Please contact the IT automation team if you require further assistance.")
    now_str = datetime.now().strftime("%d %b %Y, %H:%M:%S")
    host    = socket.gethostname()

    html = f"""<html>
<body style="font-family:Calibri,Arial,sans-serif;font-size:14px;color:#222;margin:0;padding:0;">
<table width="600" cellpadding="0" cellspacing="0" style="margin:20px auto;border:1px solid #ddd;border-radius:6px;overflow:hidden;">
  <tr><td style="background:{badge_color};padding:16px 24px;">
    <span style="font-size:20px;font-weight:bold;color:#fff;">{icon} NEXA Agent &mdash; {badge_text}</span>
  </td></tr>
  <tr><td style="padding:20px 24px 8px 24px;">
    <p style="margin:0;font-size:14px;">Dear <b>{first_name}</b>,</p>
    <p style="margin:8px 0 0 0;font-size:14px;">
      Your request for <b>{task_name}</b> has been processed successfully. Please find the details below.
    </p>
  </td></tr>
  <tr><td style="padding:12px 24px;">
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
      {table_rows_html}
    </table>
  </td></tr>
  <tr><td style="padding:8px 24px 20px 24px;">
    <p style="margin:0;font-size:13px;color:#555;">{closing}</p>
  </td></tr>
  <tr><td style="background:#f5f5f5;padding:10px 24px;border-top:1px solid #ddd;">
    <span style="font-size:11px;color:#999;">NEXA Automation Agent &nbsp;|&nbsp; {now_str} &nbsp;|&nbsp; Host: {host}</span>
  </td></tr>
</table>
</body></html>"""

    try:
        outlook = _win32.Dispatch("Outlook.Application")
        mail    = outlook.CreateItem(0)
        mail.To       = sender_email
        mail.Subject  = f"RE: {original_subject}"
        mail.HTMLBody = html
        if attachment_path and os.path.isfile(attachment_path):
            try:
                mail.Attachments.Add(attachment_path)
            except Exception as e:
                log(f"[NEXA] Could not attach file {attachment_path}: {e}")
        mail.Send()
        log(f"[NEXA] Reply sent → {sender_email}")
    except Exception as e:
        log(f"[NEXA] Reply failed → {sender_email}: {e}")

# PURPOSE: Reads task status for inclusion in a NEXA response.
def _nexa_get_status(task_id: int) -> dict:
    def _fn(conn, cur):
        cur.execute(
            "SELECT STATUS, LAST_RUN, DURATION_SEC, MESSAGE FROM TASK_STATUS "
            "WHERE TASK_ID=? ORDER BY UPDATED_TIME DESC",
            (int(task_id),)
        )
        row = cur.fetchone()
        if not row:
            return None
        status, last_run, duration_sec, message = row
        if message and " || " in str(message):
            message = str(message).split(" || ")[0]
        message = re.sub(r"All runs complete:\s*\[[\w_]+\]\s*\w+\s*in\s*[\d.]+s\s*\|\s*", "", str(message or "")).strip()
        return {
            "status"      : str(status or "UNKNOWN"),
            "last_run"    : str(last_run or "—"),
            "duration_sec": float(duration_sec) if duration_sec else None,
            "message"     : message or "—",
        }
    try:
        with db_lock:
            return _run_with_retry(_fn)
    except Exception:
        return None

# PURPOSE: Extracts the actual sender email address from an Outlook message.
def _extract_smtp_from_outlook(mail) -> str:
    try:
        if hasattr(mail, 'Sender') and mail.Sender:
            try:
                exch_user = mail.Sender.GetExchangeUser()
                if exch_user:
                    smtp = exch_user.PrimarySmtpAddress
                    if smtp:
                        return str(smtp).strip().lower()
            except Exception:
                pass
        sender_email = str(mail.SenderEmailAddress or "").strip()
        if sender_email and "@" in sender_email:
            return sender_email.lower()
        sender_name = str(mail.SenderName or "").strip()
        if sender_name and "@" in sender_name:
            email_part = sender_name.split("<")[-1].replace(">", "").strip()
            if email_part and "@" in email_part:
                return email_part.lower()
        if "/CN=" in sender_email:
            try:
                cn_part  = sender_email.split("/CN=")[-1]
                username = cn_part.split("-")[-1].lower()
                return f"{username}@ril.com"
            except Exception:
                pass
        return sender_email.lower() if sender_email else ""
    except Exception as e:
        log(f"[EMAIL] Error extracting SMTP address: {e}")
        return str(getattr(mail, 'SenderEmailAddress', '') or "").strip().lower()

# PURPOSE: Validates and processes one NEXA email command using fixed rules.
def _nexa_process_email(mail) -> None:
    subject      = str(mail.Subject or "").strip()
    sender_name  = str(mail.SenderName or "Unknown")
    sender_email = _extract_smtp_from_outlook(mail)

    parsed = _parse_nexa_subject(subject)
    if not parsed:
        return
    if not _is_sender_allowed(sender_email):
        log(f"[NEXA] ⚠️ Unauthorized sender: {sender_email}")
        return
    log(f"[NEXA] ✅ Authorized sender: {sender_email}")

    task_id, action = parsed

    if action == "TRASH_DELETE":
        log(f"[NEXA] {sender_name} → TRASH_DELETE | {subject}")
        deleted = _delete_trash_items()
        rows = [("Status", "❌ Failed to empty Trash. Please check Outlook.")] if deleted == -1 else [
            ("Action", "Delete all items in Deleted Items folder"),
            ("Items Deleted", str(deleted)),
            ("Completed At", datetime.now().strftime("%d-%m-%Y %H:%M:%S")),
        ]
        _nexa_send_reply(sender_email, sender_name, subject, "TRASH_DELETE", "Outlook Trash", rows)
        return

    if re.match(r"KEEP_INBOX_(\d+)_DAYS", action):
        days = int(re.match(r"KEEP_INBOX_(\d+)_DAYS", action).group(1))
        log(f"[NEXA] {sender_name} → KEEP_INBOX_{days}_DAYS | {subject}")
        try:
            outlook_ns = _win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
            inbox      = outlook_ns.GetDefaultFolder(6)
            items      = inbox.Items
            today      = datetime.now().date()
            cutoff     = today - timedelta(days=days)
            log(f"[NEXA] KEEP_INBOX_{days}_DAYS: Today={today}, Cutoff={cutoff}")

            # ✅ FIX: Use Restrict() to filter old emails, then delete in REVERSE
            # Iterating forward and deleting causes Outlook COM index to shift — items get skipped.
            # Restrict() gives a stable filtered collection; reverse delete avoids index shift.
            cutoff_str   = cutoff.strftime("%m/%d/%Y")
            filter_str   = f"[ReceivedTime] < '{cutoff_str} 00:00 AM'"
            old_items    = items.Restrict(filter_str)
            total_old    = old_items.Count

            # Collect subjects before deleting (COM refs become invalid after delete)
            oldest_subject = ""
            try:
                if total_old > 0:
                    oldest_subject = str(old_items.Item(1).Subject or "")[:40]
            except Exception:
                pass

            total_checked = items.Count
            deleted = 0
            # Delete in reverse — prevents index shifting mid-loop
            for i in range(total_old, 0, -1):
                try:
                    old_items.Item(i).Delete()
                    deleted += 1
                except Exception:
                    pass
            rows = [
                ("Action",         f"Keep inbox emails from last {days} calendar days"),
                ("Cutoff Date",    f"Delete emails before {cutoff}"),
                ("Emails Checked", str(total_checked)),
                ("Deleted",        str(deleted)),
                ("Oldest Deleted", oldest_subject or "—"),
                ("Result",         f"✅ {deleted} email(s) deleted successfully"),
                ("Completed At",   datetime.now().strftime("%d-%m-%Y %H:%M:%S")),
            ]
            log(f"[NEXA] KEEP_INBOX_{days}_DAYS: deleted {deleted} emails (before {cutoff})")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK",
                             f"Inbox {days}-Day Cleanup", rows)
        except Exception as e:
            log(f"[NEXA] KEEP_INBOX_{days}_DAYS error: {e}")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK",
                             f"Inbox {days}-Day Cleanup", [("Error", str(e))])
        return

    if action == "OUTBOX_AUTO":
        log(f"[NEXA] {sender_name} → OUTBOX_AUTO | {subject}")
        _set_outbox_check_mode("AUTO")
        rows = [("Action","OUTBOX_AUTO enabled"), ("Mode","Outbox will be checked every hour automatically"),
                ("Logic","If mail stuck 5 min → delete oldest 10 sent → empty trash → repeat until clear"),
                ("Alert Email", _read_agent_mode_row().get("EMAIL", "Not set")),
                ("Updated At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
        _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Outbox Auto Monitor", rows)
        return

    if action == "OUTBOX_MANUAL":
        log(f"[NEXA] {sender_name} → OUTBOX_MANUAL | {subject}")
        _set_outbox_check_mode("MANUAL")
        rows = [("Action","OUTBOX_MANUAL — auto monitoring disabled"),
                ("Mode","Outbox will no longer be checked automatically"),
                ("Updated At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
        _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Outbox Monitor Disabled", rows)
        return

    if action == "CHECK_OUTBOX":
        log(f"[NEXA] {sender_name} → CHECK_OUTBOX | {subject}")
        try:
            outlook_ns = _win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
            outbox     = outlook_ns.GetDefaultFolder(4)
            count      = outbox.Items.Count
            status     = "✅ Empty — All emails sent" if count == 0 else f"⚠️ {count} mail(s) stuck in Outbox"
            rows = [("Outbox Count", str(count)), ("Status", status),
                    ("Checked At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
            if count > 0:
                items = outbox.Items
                for i in range(1, min(count + 1, 6)):
                    try:
                        m = items.Item(i)
                        rows.append((f"Stuck Mail {i}", f"{str(m.Subject or '')[:40]} → {str(m.To or '')[:30]}"))
                    except Exception:
                        pass
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Outbox Status", rows)
        except Exception as e:
            log(f"[NEXA] CHECK_OUTBOX error: {e}")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Outbox Status",
                             [("Status", "❌ Cannot connect to Outlook"), ("Error", str(e))])
        return

    if action == "CHECK_OUTLOOK":
        log(f"[NEXA] {sender_name} → CHECK_OUTLOOK | {subject}")
        try:
            outlook_app  = _win32.Dispatch("Outlook.Application")
            outlook_ns   = outlook_app.GetNamespace("MAPI")
            inbox        = outlook_ns.GetDefaultFolder(6)
            outbox       = outlook_ns.GetDefaultFolder(4)
            sent         = outlook_ns.GetDefaultFolder(5)
            outbox_count = outbox.Items.Count
            rows = [
                ("Outlook Status", "✅ Connected"),
                ("Checked At",     datetime.now().strftime("%d-%m-%Y %H:%M:%S")),
                ("Inbox Total",    str(inbox.Items.Count)),
                ("Inbox Unread",   str(inbox.UnReadItemCount)),
                ("Outbox",         "✅ Empty" if outbox_count == 0 else f"⚠️ {outbox_count} mail(s) stuck"),
                ("Sent Items",     str(sent.Items.Count)),
            ]
            if outbox_count > 0:
                items = outbox.Items
                for i in range(1, min(outbox_count + 1, 6)):
                    try:
                        m = items.Item(i)
                        rows.append((f"Stuck Mail {i}", f"{str(m.Subject or '')[:40]} → {str(m.To or '')[:30]}"))
                    except Exception:
                        pass
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Outlook Status", rows)
        except Exception as e:
            log(f"[NEXA] CHECK_OUTLOOK error: {e}")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Outlook Status",
                             [("Outlook Status", "❌ Cannot connect to Outlook"), ("Error Details", str(e))])
        return

    if action == "KEEP_OUTBOX":
        keep_count = task_id
        log(f"[NEXA] {sender_name} → KEEP_OUTBOX {keep_count} | {subject}")
        try:
            outlook_ns = _win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
            outbox     = outlook_ns.GetDefaultFolder(4)
            items      = outbox.Items
            items.Sort("[ReceivedTime]", True)
            total = items.Count
            if total <= keep_count:
                rows = [("Action", f"KEEP latest {keep_count} outbox mail(s)"),
                        ("Outbox Total", str(total)),
                        ("Result", f"✅ Nothing deleted — only {total} mail(s) in outbox"),
                        ("Checked At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
            else:
                to_delete = []
                for i in range(keep_count + 1, total + 1):
                    try:
                        to_delete.append(items.Item(i))
                    except Exception:
                        pass
                deleted = 0
                for mail_item in to_delete:
                    try:
                        mail_item.Delete()
                        deleted += 1
                    except Exception:
                        pass
                rows = [("Action", f"KEEP latest {keep_count} outbox mail(s)"),
                        ("Outbox Total", str(total)), ("Kept", str(keep_count)),
                        ("Deleted", str(deleted)),
                        ("Result", f"✅ {deleted} older outbox mail(s) deleted"),
                        ("Completed At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
                log(f"[NEXA] KEEP_OUTBOX: kept {keep_count}, deleted {deleted} of {total}")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Outbox Cleanup", rows)
        except Exception as e:
            log(f"[NEXA] KEEP_OUTBOX error: {e}")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Outbox Cleanup", [("Error", str(e))])
        return

    if action == "DELETE_INBOX":
        delete_count = task_id
        log(f"[NEXA] {sender_name} → DELETE_INBOX {delete_count} | {subject}")
        try:
            outlook_ns = _win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
            inbox      = outlook_ns.GetDefaultFolder(6)
            items      = inbox.Items
            items.Sort("[ReceivedTime]", False)
            total = items.Count
            if total == 0:
                rows = [("Action", f"DELETE oldest {delete_count} inbox mail(s)"),
                        ("Inbox Total", "0"), ("Result", "✅ Inbox is empty — nothing to delete"),
                        ("Checked At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
            else:
                actual_delete = min(delete_count, total)
                to_delete = []
                for i in range(1, actual_delete + 1):
                    try:
                        to_delete.append(items.Item(i))
                    except Exception:
                        pass
                deleted = 0
                oldest_sub = ""
                newest_sub = ""
                for idx, mail_item in enumerate(to_delete):
                    try:
                        if idx == 0:
                            oldest_sub = str(mail_item.Subject or "")[:40]
                        newest_sub = str(mail_item.Subject or "")[:40]
                        mail_item.Delete()
                        deleted += 1
                    except Exception:
                        pass
                rows = [("Action", f"DELETE oldest {delete_count} inbox mail(s)"),
                        ("Inbox Total", str(total)), ("Deleted", str(deleted)),
                        ("Oldest Deleted", oldest_sub or "—"), ("Newest Deleted", newest_sub or "—"),
                        ("Result", f"✅ {deleted} oldest inbox mail(s) deleted"),
                        ("Completed At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
                log(f"[NEXA] DELETE_INBOX: deleted {deleted} oldest of {total} inbox mails")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Inbox Cleanup", rows)
        except Exception as e:
            log(f"[NEXA] DELETE_INBOX error: {e}")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Inbox Cleanup", [("Error", str(e))])
        return

    if action == "DELETE_SENT":
        delete_count = task_id
        log(f"[NEXA] {sender_name} → DELETE_SENT {delete_count} | {subject}")
        try:
            outlook_ns = _win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
            sent       = outlook_ns.GetDefaultFolder(5)
            items      = sent.Items
            items.Sort("[SentOn]", False)
            total = items.Count
            if total == 0:
                rows = [("Action", f"DELETE oldest {delete_count} sent mails"),
                        ("Sent Total", "0"), ("Result", "✅ Sent folder is empty — nothing to delete"),
                        ("Checked At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
            else:
                actual_delete = min(delete_count, total)
                to_delete = []
                for i in range(1, actual_delete + 1):
                    try:
                        to_delete.append(items.Item(i))
                    except Exception:
                        pass
                deleted = 0
                oldest_sub = ""
                newest_sub = ""
                for idx, mail_item in enumerate(to_delete):
                    try:
                        if idx == 0:
                            oldest_sub = str(mail_item.Subject or "")[:40]
                        newest_sub = str(mail_item.Subject or "")[:40]
                        mail_item.Delete()
                        deleted += 1
                    except Exception:
                        pass
                rows = [("Action", f"DELETE oldest {delete_count} sent mails"),
                        ("Sent Total", str(total)), ("Deleted", str(deleted)),
                        ("Oldest Deleted", oldest_sub or "—"), ("Newest Deleted", newest_sub or "—"),
                        ("Result", f"✅ {deleted} oldest sent mail(s) deleted successfully"),
                        ("Completed At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
                log(f"[NEXA] DELETE_SENT: deleted {deleted} oldest of {total} sent mails")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Sent Cleanup", rows)
        except Exception as e:
            log(f"[NEXA] DELETE_SENT error: {e}")
            _nexa_send_reply(sender_email, sender_name, subject, "OUTLOOK", "Sent Cleanup", [("Error", str(e))])
        return

    # ── Task-specific commands ────────────────────────────────────────────────
    log(f"[NEXA] {sender_name} → {action} TASK_{task_id} | {subject}")

    if action in ("ENABLE", "DISABLE"):
        def _get_any_task(conn, cur):
            cur.execute("SELECT * FROM TASK_MASTER WHERE TASK_ID=?", (int(task_id),))
            cols = [c[0] for c in cur.description]
            row  = cur.fetchone()
            return dict(zip(cols, row)) if row else None
        try:
            with db_lock:
                task = _run_with_retry(_get_any_task)
        except Exception:
            task = None
    else:
        with _cache_lock:
            task = next((t for t in _cached_tasks if int(t.get("TASK_ID", 0)) == task_id), None)

    if not task:
        log(f"[NEXA] Task {task_id} not found")
        _nexa_send_reply(sender_email, sender_name, subject, "STATUS", f"TASK_{task_id}",
                         [("Result", f"❌ TASK {task_id} not found in TASK_MASTER.")])
        return

    task_name   = task.get("TASK_NAME", f"TASK_{task_id}")
    detail_rows = []
    now_str     = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    if action == "RUN":
        def _set_run(conn, cur):
            cur.execute("UPDATE TASK_MASTER SET MANUAL_ACTION='RUN' WHERE TASK_ID=?", (int(task_id),))
        try:
            with db_lock:
                _run_with_retry(_set_run)
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                           ("Action", "RUN"), ("Status", "Queued for execution"),
                           ("Requested At", now_str)]
        except Exception as e:
            detail_rows = [("Error", str(e))]

    elif action == "E_RUN":
        def _set_erun(conn, cur):
            cur.execute("UPDATE TASK_MASTER SET MANUAL_ACTION='E_RUN' WHERE TASK_ID=?", (int(task_id),))
        try:
            with db_lock:
                _run_with_retry(_set_erun)
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                           ("Action", "E_RUN (Kill + Restart)"),
                           ("Status", "Killing current run (if any) and restarting"),
                           ("Requested At", now_str)]
        except Exception as e:
            detail_rows = [("Error", str(e))]

    elif action == "KILL":
        with processes_lock:
            targets = {rk: info for rk, info in running_processes.items()
                       if str(info["task"]["TASK_ID"]) == str(task_id)}
        if not targets:
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                           ("Action", "KILL"), ("Result", "Task was not running at the time of request")]
        else:
            killed = 0
            for rk, info in targets.items():
                p = info["process"]
                try:
                    parent = psutil.Process(p.pid)
                    for child in parent.children(recursive=True):
                        child.kill()
                    parent.kill()
                    killed += 1
                except psutil.NoSuchProcess:
                    pass
            with processes_lock:
                for rk in targets:
                    running_processes.pop(rk, None)
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                           ("Action", "KILL"), ("Instances Stopped", str(killed)),
                           ("Terminated At", now_str)]

    elif action == "STATUS":
        s = _nexa_get_status(task_id)
        if s:
            dur = f"{s['duration_sec']:.0f}s ({s['duration_sec']/60:.1f} min)" if s["duration_sec"] else "—"
            msg = s["message"]
            msg = re.sub(r"\[[\w_]+\]\s+\w+\s+in\s+[\d.]+s\s*\|\s*", "", msg).strip()
            msg = msg.replace("All runs complete:", "").strip(" |")
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                           ("Current Status", f"<b>{s['status']}</b>"), ("Last Run", s["last_run"]),
                           ("Duration", dur), ("Message", msg or "—")]
        else:
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                           ("Status", "No run history found")]

    elif action == "LOG":
        try:
            log_path, created_time, log_updated = _read_log_times(task_id)
            if not log_path:
                detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                               ("Log Path", "Not configured in LOG_PATHS table"),
                               ("Checked At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
            else:
                live_mtime = _get_log_mtime(log_path)
                live_str   = datetime.fromtimestamp(live_mtime).strftime("%d-%m-%Y %H:%M:%S") if live_mtime else "No file found"
                detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                               ("Log Path", log_path),
                               ("Task Started At", created_time or "Not recorded yet"),
                               ("Log Updated At",  log_updated  or "Not recorded yet"),
                               ("Live File Time",  live_str),
                               ("Checked At", datetime.now().strftime("%d-%m-%Y %H:%M:%S"))]
        except Exception as e:
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)), ("Error", str(e))]

    elif action == "SEND_LOG_FILE":
        attachment_path = None
        try:
            log_path, created_time, log_updated = _read_log_times(task_id)
            if not log_path:
                detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                               ("Result", "❌ LOG_PATH not configured in LOG_PATHS table")]
            else:
                latest_file = None
                if os.path.isfile(log_path):
                    latest_file = log_path
                elif os.path.isdir(log_path):
                    candidates = [os.path.join(log_path, f) for f in os.listdir(log_path)
                                  if os.path.isfile(os.path.join(log_path, f))]
                    if candidates:
                        latest_file = max(candidates, key=os.path.getmtime)
                if not latest_file:
                    detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                                   ("Log Path", log_path),
                                   ("Result", "❌ No log file found in the configured path")]
                else:
                    mtime     = os.path.getmtime(latest_file)
                    mtime_str = datetime.fromtimestamp(mtime).strftime("%d-%m-%Y %H:%M:%S")
                    size_kb   = os.path.getsize(latest_file) / 1024
                    attachment_path = latest_file
                    detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                                   ("File Name",     os.path.basename(latest_file)),
                                   ("File Size",     f"{size_kb:.1f} KB"),
                                   ("Last Modified", mtime_str),
                                   ("Result",        "✅ Latest log file attached to this email")]
                    log(f"[NEXA] SEND_LOG_FILE: attaching {latest_file}")
        except Exception as e:
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)), ("Error", str(e))]
        _nexa_send_reply(sender_email, sender_name, subject, "SEND_LOG_FILE",
                         task_name, detail_rows, attachment_path=attachment_path)
        return

    elif action == "ENABLE":
        def _enable(conn, cur):
            cur.execute("UPDATE TASK_MASTER SET ENABLED='Y' WHERE TASK_ID=?", (int(task_id),))
        try:
            with db_lock:
                _run_with_retry(_enable)
            with _cache_lock:
                for t in _cached_tasks:
                    if int(t.get("TASK_ID", 0)) == task_id:
                        t["ENABLED"] = "Y"
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                           ("Action", "ENABLE"), ("New Status", "Enabled — Active in Scheduler"),
                           ("Updated At", now_str)]
        except Exception as e:
            detail_rows = [("Error", str(e))]

    elif action == "DISABLE":
        def _disable(conn, cur):
            cur.execute("UPDATE TASK_MASTER SET ENABLED='N' WHERE TASK_ID=?", (int(task_id),))
        try:
            with db_lock:
                _run_with_retry(_disable)
            with _cache_lock:
                for t in _cached_tasks:
                    if int(t.get("TASK_ID", 0)) == task_id:
                        t["ENABLED"] = "N"
            detail_rows = [("Task Name", task_name), ("Task ID", str(task_id)),
                           ("Action", "DISABLE"), ("New Status", "Disabled — Removed from Scheduler"),
                           ("Updated At", now_str)]
        except Exception as e:
            detail_rows = [("Error", str(e))]

    else:
        detail_rows = [("Error", f"Unknown action: {action}")]

    _nexa_send_reply(sender_email, sender_name, subject, action, task_name, detail_rows)

# PURPOSE: Attempts to dismiss an Outlook security popup if present.
def _dismiss_outlook_security_popup() -> bool:
    """
    Detects and auto-clicks 'Yes' on the 'Internet Security Warning' popup
    that sometimes appears when Outlook opens. Uses Win32 EnumWindows to
    find the popup by title, then sends a click to the Yes button.
    
    Returns True if popup was found and dismissed, False if no popup found.
    """
    try:
        import win32gui
        import win32con
    except ImportError:
        return False
    
    popup_hwnd = [None]
    
    # Known popup titles that appear from Outlook
    POPUP_TITLES = [
        "Internet Security Warning",
        "Security Warning",
        "Windows Security",
    ]
    
    def _enum_callback(hwnd, _):
        if not win32gui.IsWindowVisible(hwnd):
            return True
        title = win32gui.GetWindowText(hwnd)
        for popup_title in POPUP_TITLES:
            if popup_title.lower() in title.lower():
                popup_hwnd[0] = hwnd
                return False  # stop enumeration
        return True
    
    try:
        win32gui.EnumWindows(_enum_callback, None)
    except Exception:
        return False
    
    if not popup_hwnd[0]:
        return False  # no popup found
    
    hwnd = popup_hwnd[0]
    title = win32gui.GetWindowText(hwnd)
    log(f"[NEXA] Detected popup: '{title}' — auto-clicking Yes")
    
    # Find the "Yes" button inside the popup and click it
    yes_button = [None]
    
    def _find_yes_button(child_hwnd, _):
        try:
            btn_text = win32gui.GetWindowText(child_hwnd)
            if btn_text.strip().lower() in ("&yes", "yes"):
                yes_button[0] = child_hwnd
                return False
        except Exception:
            pass
        return True
    
    try:
        win32gui.EnumChildWindows(hwnd, _find_yes_button, None)
    except Exception:
        pass
    
    if yes_button[0]:
        try:
            # Click the Yes button by sending BM_CLICK message
            win32gui.SendMessage(yes_button[0], win32con.BM_CLICK, 0, 0)
            log(f"[NEXA] Popup dismissed (clicked Yes)")
            return True
        except Exception as e:
            log(f"[NEXA] Failed to click Yes: {e}")
    
    # Fallback: send Enter key to the popup (Yes is usually default)
    try:
        win32gui.SetForegroundWindow(hwnd)
        time.sleep(0.3)
        win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0)
        win32gui.PostMessage(hwnd, win32con.WM_KEYUP, win32con.VK_RETURN, 0)
        log(f"[NEXA] Popup dismissed (sent Enter key)")
        return True
    except Exception as e:
        log(f"[NEXA] Failed to dismiss popup: {e}")
        return False


# PURPOSE: Checks Outlook availability and starts it if needed.
def _ensure_outlook_running() -> bool:
    outlook_running = any(p.name().lower() == "outlook.exe" for p in psutil.process_iter(["name"]))
    if not outlook_running:
        log("[NEXA] Outlook not running — launching it...")
        try:
            subprocess.Popen(
                "C:\\Program Files\\Microsoft Office\\root\\Office16\\OUTLOOK.EXE",
                shell=False
            )
        except FileNotFoundError:
            try:
                subprocess.Popen("outlook.exe", shell=True)
            except Exception as e:
                log(f"[NEXA] Could not launch Outlook: {e}")
                return False
        for _ in range(30):
            time.sleep(1)
            running = any(p.name().lower() == "outlook.exe" for p in psutil.process_iter(["name"]))
            if running:
                log("[NEXA] Outlook launched — waiting 10s for it to be ready")
                time.sleep(10)
                # Check for security popup and auto-dismiss if present
                # Try up to 5 times over 15 seconds (popup may appear late)
                for attempt in range(5):
                    if _dismiss_outlook_security_popup():
                        time.sleep(2)  # let popup close
                        break
                    time.sleep(3)
                return True
        log("[NEXA] Outlook did not start in time")
        return False
    else:
        # Outlook already running — still check for stuck popup (defensive)
        _dismiss_outlook_security_popup()
    return True

# PURPOSE: Scans inbox messages and processes recognized NEXA requests.
def _nexa_scan_inbox() -> None:
    import pythoncom
    if not _ensure_outlook_running():
        log("[NEXA] Skipping scan — Outlook unavailable")
        return
    try:
        pythoncom.CoInitialize()
        outlook = _win32.Dispatch("Outlook.Application")
        ns      = outlook.GetNamespace("MAPI")
        ns.Logon()
        inbox   = ns.GetDefaultFolder(6)
        items   = inbox.Items
        items.Sort("[ReceivedTime]", True)
    except Exception as e:
        log(f"[NEXA] Inbox access failed: {e}")
        raise

    processed = 0
    try:
        for i in range(1, min(NEXA_SCAN_LIMIT + 1, items.Count + 1)):
            try:
                mail = items.Item(i)
                if not mail.UnRead:
                    continue
                subject = str(mail.Subject or "")
                # Skip silently if no NEXA: in subject — not our email at all
                if not re.search(r"NEXA:", subject, re.IGNORECASE):
                    continue
                # Skip RE: replies — only process original NEXA commands
                if re.match(r"^RE:\s*", subject, re.IGNORECASE):
                    # Mark as read so it doesn't keep showing up
                    try:
                        mail.UnRead = False
                        mail.Save()
                    except Exception:
                        pass
                    continue
                log(f"[NEXA INBOX] Found unread NEXA email: {subject}")
                try:
                    _nexa_process_email(mail)
                    mail.UnRead = False
                    mail.Save()
                    log(f"[NEXA] Marked as read: {subject}")
                except Exception as e:
                    log(f"[NEXA] Processing failed — email remains unread for retry: {e}")
                    continue
                processed += 1
                if processed >= NEXA_SCAN_LIMIT:
                    break
            except Exception as e:
                log(f"[NEXA] Email processing error: {e}")
                continue
    except Exception as e:
        log(f"[NEXA] Scan error: {e}")

# PURPOSE: Periodically scans the inbox when the NEXA agent is enabled.
def nexa_loop():
    import pythoncom
    pythoncom.CoInitialize()
    log("[NEXA] Thread started — waiting for AGENT_MODE = ENABLE")
    log("[NEXA] 🔄 Will PAUSE email scanning when tasks are running, resume when idle")

    fail_count         = 0
    MAX_FAILS          = 3
    RETRY_WAIT         = 60
    tasks_were_running = False

    while APP_RUNNING:
        with _agent_lock:
            active = _agent_enabled

        if active:
            with processes_lock:
                tasks_running = len(running_processes) > 0

            if tasks_running:
                if not tasks_were_running:
                    tasks_were_running = True
                time.sleep(NEXA_POLL_INTERVAL)
                continue
            else:
                if tasks_were_running:
                    tasks_were_running = False

            try:
                _nexa_scan_inbox()
                fail_count = 0
            except Exception as e:
                fail_count += 1
                log(f"[NEXA] Loop error #{fail_count}: {e}")
                if fail_count >= MAX_FAILS:
                    log(f"[NEXA] {MAX_FAILS} consecutive failures — waiting {RETRY_WAIT}s before retry")
                    _sleep_checked(RETRY_WAIT)
                    fail_count = 0
                    try:
                        pythoncom.CoUninitialize()
                        pythoncom.CoInitialize()
                        log("[NEXA] COM reinitialized — resuming")
                    except Exception:
                        pass

        time.sleep(NEXA_POLL_INTERVAL)

# ─── Keepalive ─────────────────────────────────────────────────────────────────

# PURPOSE: Keeps the parent process alive and reports scheduler health.
def _keepalive_loop():
    while APP_RUNNING:
        time.sleep(60)
    log("[KEEPALIVE] APP_RUNNING=False — keepalive thread exiting")

# ─── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":

    log("=" * 60)
    log("Rule-Based LangGraph SQLite Task Scheduler + NEXA Agent — CRASH-PROTECTED VERSION (NO LLM)")
    log(f"PID={os.getpid()}  Host={socket.gethostname()}")
    log("=" * 60)

    _install_crash_protection()
    _init_sqlite()
    _ensure_agent_mode_table()
    reset_stale_running()

    threading.Thread(target=_keepalive_loop,       daemon=False, name="Keepalive").start()
    threading.Thread(target=monitor_loop,           daemon=True,  name="Monitor").start()
    threading.Thread(target=_refresh_cache,         daemon=True,  name="CacheRefresh").start()
    threading.Thread(target=scheduler_loop,         daemon=True,  name="Scheduler").start()
    threading.Thread(target=_log_scheduler_status,  daemon=True,  name="StatusLog").start()
    threading.Thread(target=manual_action_loop,     daemon=True,  name="ManualAction").start()
    threading.Thread(target=heartbeat_loop,         daemon=True,  name="Heartbeat").start()
    threading.Thread(target=nexa_loop,              daemon=True,  name="NEXA").start()
    threading.Thread(target=log_monitor_loop,       daemon=True,  name="LogMonitor").start()
    threading.Thread(target=outbox_monitor_loop,    daemon=True,  name="OutboxMonitor").start()
    threading.Thread(target=trash_cleaner_loop,     daemon=True,  name="TrashCleaner").start()

    log("All threads started. Rule-based LangGraph scheduler + log-decision graph active; no LLM/API key is used. Press Ctrl+C to stop.")
    log("✅ CRASH-PROTECTION ACTIVE:")
    log("  - Signal handlers: SIGTERM, SIGBREAK, CTRL_CLOSE/LOGOFF/SHUTDOWN")
    log("  - atexit handler: logs on interpreter exit")
    log("  - threading.excepthook: catches unhandled thread exceptions")
    log("  - Non-daemon keepalive thread: prevents silent exit")
    log("✅ BUG FIXES APPLIED:")
    log("  B1. _watch_log_after_spawn: STEP1/STEP2 diagnostic logs, non-fatal")
    log("      _write_log_times, print()+log() in all excepts, per-run guard,")
    log("      poll heartbeat every 10 polls (~100s)")
    log("  B2. scheduler_loop: _detect_and_clear_schedule_changes() clears stale")
    log("      _fired_slots within 1s of cache refresh when WINDOW_START changes")
    log("NEXA: Set AGENT_MODE.AGENT_MODE = 'ENABLE' to activate email watching.")
    log("NEXA: 🔄 Scanning automatically PAUSES when tasks are running.")

    try:
        while APP_RUNNING:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown("KeyboardInterrupt (Ctrl+C)")
    except SystemExit as e:
        _shutdown(f"SystemExit (code={e.code})")
    except BaseException as e:
        _shutdown(f"Unexpected BaseException: {type(e).__name__}: {e}")
    finally:
        if APP_RUNNING:
            _shutdown("Main loop exited unexpectedly (APP_RUNNING was still True)")
        log("[MAIN] ✅ Process exit complete")
