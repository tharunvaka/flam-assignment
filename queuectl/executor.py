#Done By Vaka Tharun Kumar
import subprocess
import time
import signal
import os
from datetime import datetime, timedelta, timezone
from queuectl.persistence import TaskLedger, KEY_BACKOFF_BASE, KEY_RETRY_LIMIT

class TaskExecutor:
    def __init__(self):
        self.taskLog = TaskLedger()
        self.sleepGap = 1  
        self.exitFlag = False 
        self.activePid = None

    def setupSHandlers(self):
        signal.signal(signal.SIGINT, self.exitt)
        signal.signal(signal.SIGTERM, self.exitt)

    def exitt(self, signum, frame):
        print(f"Worker (PID: {os.getpid()}) received shutdown signal. Finishing current task...")
        self.exitFlag = True
        if self.activePid:
            try:
                os.kill(self.activePid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def beginProcess(self):
        print(f"Worker started (PID: {os.getpid()})")
        while not self.exitFlag:
            work_unit = self.taskLog.acquire_next_task()
            if work_unit:
                self.execTask(work_unit)
            else:
                time.sleep(self.sleepGap)
        
        print(f"Worker (PID: {os.getpid()}) shutting down.")

    def execTask(self, work_unit):
        task_id = work_unit["id"]
        command = work_unit["command"]
        
        print(f"[PID: {os.getpid()}] Processing task: {task_id} ('{command}')")

        try:
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.activePid = process.pid
            process.wait()
            self.activePid = None
            ecode = process.returncode
            
            if ecode == 0:
                self.taskLog.update_task_on_success(task_id)
                print(f"[PID: {os.getpid()}] Task {task_id} completed successfully.")
            else:
                errlog = process.stderr.read().decode('utf-8').strip()
                print(f"[PID: {os.getpid()}] Task {task_id} failed with exit code {ecode}. Error: {errlog}")
                self.processFailure(work_unit)

        except Exception as e:
            print(f"[PID: {os.getpid()}] Task {task_id} failed critically. Error: {e}")
            self.processFailure(work_unit)
        
        finally:
            self.activePid = None 

    def processFailure(self, taInfo):
        tid = taInfo["id"]
        attemptsCount = taInfo["execution_count"] + 1
        rlimit = int(self.taskLog.get_config_value(KEY_RETRY_LIMIT, 3))
        
        if attemptsCount >= rlimit:
            self.taskLog.move_task_to_dlq(tid, attemptsCount) 
            print(f"[PID: {os.getpid()}] Task {tid} moved to DLQ after {attemptsCount} attempts.")
        else:
            base = int(self.taskLog.get_config_value(KEY_BACKOFF_BASE, 2))
            dsec = base ** attemptsCount
            
            nrt = datetime.now(timezone.utc) + timedelta(seconds=dsec)
            self.taskLog.update_task_on_failure(tid, attemptsCount, nrt.isoformat())
            print(f"[PID: {os.getpid()}] Task {tid} will be retried after {dsec}s.")