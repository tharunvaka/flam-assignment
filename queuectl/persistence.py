#Done By Vaka Tharun Kumar
import sqlite3
import os
import uuid
from datetime import datetime, timezone

DBNAME = "task_queue.db"
LDBPATH = os.path.join(os.getcwd(), DBNAME)
KEY_RETRY_LIMIT = "CEILING_RETRY_ATTEMPTS"
KEY_BACKOFF_BASE = "RETRY_EXP_BASE"
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_DEAD = "dead"

def conntoDB():
    conn = sqlite3.connect(LDBPATH)
    conn.row_factory = sqlite3.Row
    return conn

class TaskLedger:
    def __init__(self):
        self.setupDB()

    def setupDB(self):
        with conntoDB() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS work_units (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                status_flag TEXT NOT NULL DEFAULT 'pending',
                execution_count INTEGER NOT NULL DEFAULT 0,
                retry_limit INTEGER NOT NULL,
                next_run_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS configuration (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """)

            cursor.execute(
                "INSERT OR IGNORE INTO configuration (key, value) VALUES (?, ?)",
                (KEY_RETRY_LIMIT, "3")
            )
            cursor.execute(
                "INSERT OR IGNORE INTO configuration (key, value) VALUES (?, ?)",
                (KEY_BACKOFF_BASE, "2")
            )
            conn.commit()

    def get_config_value(self, key, default=None):
        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM configuration WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row["value"] if row else default

    def set_config_value(self, key, value):
        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO configuration (key, value) VALUES (?, ?)",
                (key, str(value))
            )
            conn.commit()

    def submit_new_task(self, cmdStr, job_id=None):
        ctISO = datetime.now(timezone.utc).isoformat()
        taskId = job_id if job_id else str(uuid.uuid4())
        
        defaRetries = int(self.get_config_value(KEY_RETRY_LIMIT, 3))

        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO work_units 
                (id, command, status_flag, execution_count, retry_limit, next_run_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                taskId,
                cmdStr,
                STATUS_PENDING,
                0,
                defaRetries,
                ctISO,
                ctISO,
                ctISO
            ))
            conn.commit()
        return taskId

    def acquire_next_task(self):
        conn = conntoDB()
        cursor = conn.cursor()
        
        try:
            cursor.execute("BEGIN IMMEDIATE")
            
            current_time_iso = datetime.now(timezone.utc).isoformat()
            
            cursor.execute("""
            SELECT * FROM work_units
            WHERE (status_flag = ? OR (status_flag = ? AND next_run_at <= ?))
            ORDER BY created_at ASC
            LIMIT 1
            """, (STATUS_PENDING, STATUS_FAILED, current_time_iso))
            
            row = cursor.fetchone()

            if not row:
                conn.commit()
                conn.close()
                return None

            task = dict(row)
            task_id = task["id"]
            
            cursor.execute("""
            UPDATE work_units
            SET status_flag = ?, updated_at = ?
            WHERE id = ?
            """, (STATUS_PROCESSING, current_time_iso, task_id))
            
            conn.commit()
            return task
        
        except sqlite3.OperationalError as e:
            conn.rollback()
            return None
        finally:
            if conn:
                conn.close()

    def update_task_on_success(self, task_id):
        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE work_units
            SET status_flag = ?, updated_at = ?
            WHERE id = ?
            """, (STATUS_COMPLETED, datetime.now(timezone.utc).isoformat(), task_id))
            conn.commit()

    def update_task_on_failure(self, task_id, current_attempts, next_run_iso):
        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE work_units
            SET status_flag = ?, execution_count = ?, next_run_at = ?, updated_at = ?
            WHERE id = ?
            """, (
                STATUS_FAILED,
                current_attempts,
                next_run_iso,
                datetime.now(timezone.utc).isoformat(),
                task_id
            ))
            conn.commit()
    def move_task_to_dlq(self, task_id, final_attempt_count):
        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE work_units
            SET status_flag = ?, execution_count = ?, updated_at = ?
            WHERE id = ?
            """, (
                STATUS_DEAD,
                final_attempt_count,
                datetime.now(timezone.utc).isoformat(),
                task_id
            ))
            conn.commit()

    def get_status_summary(self):
        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT status_flag, COUNT(*) as count
            FROM work_units
            GROUP BY status_flag
            """)
            return {row["status_flag"]: row["count"] for row in cursor.fetchall()}

    def find_tasks_by_status(self, status):
        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM work_units WHERE status_flag = ?", (status,))
            return [dict(row) for row in cursor.fetchall()]

    def retry_dead_task(self, task_id):
        with conntoDB() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM work_units WHERE id = ? AND status_flag = ?",
                (task_id, STATUS_DEAD)
            )
            row = cursor.fetchone()            
            if not row:
                return False
            cursor.execute("""
            UPDATE work_units
            SET status_flag = ?, execution_count = 0, updated_at = ?
            WHERE id = ?
            """, (STATUS_PENDING, datetime.now(timezone.utc).isoformat(), task_id))
            conn.commit()
            return True