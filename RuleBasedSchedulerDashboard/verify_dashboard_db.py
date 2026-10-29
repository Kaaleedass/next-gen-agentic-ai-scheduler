"""Read-only diagnostic for Scheduler Studio and the running LangGraph scheduler.

Run with the same Python environment used on Windows:
    python verify_dashboard_db.py
Optional override:
    set SCHEDULER_DB_PATH=C:\\Users\\Kaale\\Desktop\\MY_Personal_project\\Task_scheduler_agent\\TASK_TABLE\\scheduler.db
The script does not modify the database.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(os.getenv("SCHEDULER_DB_PATH", r"C:\Users\Kaale\Desktop\MY_Personal_project\Task_scheduler_agent\TASK_TABLE\scheduler.db"))


def main() -> int:
    print("Scheduler Studio SQLite diagnostic")
    print("=" * 38)
    print(f"Configured DB path : {DB_PATH}")
    print(f"Resolved DB path   : {DB_PATH.resolve()}")
    print(f"File exists        : {DB_PATH.is_file()}")
    if not DB_PATH.is_file():
        print("ERROR: database file not found. This diagnostic will not create a new database.")
        return 2
    print(f"File size          : {DB_PATH.stat().st_size:,} bytes")

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            print(f"SQLite integrity   : {conn.execute('PRAGMA quick_check').fetchone()[0]}")
            tables = {str(r[0]).upper() for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            print(f"TASK_MASTER exists : {'TASK_MASTER' in tables}")
            if "TASK_MASTER" not in tables:
                print("ERROR: TASK_MASTER not found in this file.")
                return 3
            count = conn.execute("SELECT COUNT(*) FROM TASK_MASTER").fetchone()[0]
            print(f"TASK_MASTER rows   : {count}")
            columns = [str(r[1]) for r in conn.execute("PRAGMA table_info(TASK_MASTER)").fetchall()]
            print(f"TASK_MASTER columns: {', '.join(columns)}")
            if "TASK_MASTER_LOCAL" in tables:
                cache = conn.execute("SELECT COUNT(*) FROM TASK_MASTER_LOCAL").fetchone()[0]
                print(f"Scheduler cache rows: {cache}")
                print("Tasks in TASK_MASTER but not yet in TASK_MASTER_LOCAL:")
                rows = conn.execute("""
                    SELECT tm.TASK_ID, tm.TASK_NAME, tm.ENABLED
                    FROM TASK_MASTER tm
                    LEFT JOIN TASK_MASTER_LOCAL l ON l.TASK_ID=tm.TASK_ID
                    WHERE l.TASK_ID IS NULL
                    ORDER BY tm.TASK_ID
                """).fetchall()
                if rows:
                    for row in rows:
                        print(f"  ID={row['TASK_ID']} | {row['TASK_NAME']} | enabled={row['ENABLED']}")
                else:
                    print("  (none)")
            else:
                print("Scheduler cache rows: TASK_MASTER_LOCAL table is missing. Start the LangGraph scheduler and let it initialize.")
            if "TASK_OUTPUT_CONFIG" in tables:
                print(f"Output path configs : {conn.execute('SELECT COUNT(*) FROM TASK_OUTPUT_CONFIG').fetchone()[0]}")
            print("\nLatest task rows:")
            for row in conn.execute("SELECT TASK_ID,TASK_NAME,SCRIPT_PATH,ENABLED,MANUAL_ACTION FROM TASK_MASTER ORDER BY TASK_ID DESC LIMIT 15"):
                print(f"  ID={row['TASK_ID']} | {row['TASK_NAME']} | enabled={row['ENABLED']} | action={row['MANUAL_ACTION'] or '-'}")
            print("\nCompare the resolved path above with /api/health and the scheduler's SQLITE_FILE setting.")
            return 0
        finally:
            conn.close()
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}")
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
