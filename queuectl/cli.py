#Done By Vaka Tharun Kumar
import click
import os
import signal
import json                                 
import time
from multiprocessing import Process
import subprocess
from queuectl.persistence import TaskLedger, STATUS_DEAD, STATUS_PENDING, KEY_RETRY_LIMIT, KEY_BACKOFF_BASE, STATUS_PROCESSING, STATUS_COMPLETED, STATUS_FAILED
from queuectl.executor import TaskExecutor

PIDD = os.path.join(os.getcwd(), ".queuectl_pids") 
def startWP():
    exe = TaskExecutor()
    signal.signal(signal.SIGINT, exe.exitt)
    signal.signal(signal.SIGTERM, exe.exitt)
    exe.beginProcess()

def getRPids():
    if not os.path.exists(PIDD):
        return []                               
    try:
        with open(PIDD, 'r') as pf:
            return [int(personidd) for personidd in pf.read().splitlines() if personidd]
    except (IOError, ValueError):
        return []

def saveRPids(plistt):
    try:
        with open(PIDD, 'w') as ff:
            for psave in plistt:
                ff.write(f"{psave}\n")
    except IOError as e:
        click.echo(f"Error writing to PID file: {e}")

@click.group()
def main_cli():
    TaskLedger().setupDB()

@main_cli.command()
@click.argument('job_payload', type=str)
def enqueue(job_payload):
    jsonipt = job_payload 
    try:
        jdata = json.loads(jsonipt)
        c2r = jdata.get("command")                 
        ujid = jdata.get("id")
        if not c2r:
            click.echo("Error: 'command' field is required in JSON payload.", err=True)
            return
            
        dbmanager = TaskLedger()
        nid = dbmanager.submit_new_task(c2r, ujid)
        click.echo(f"Job enqueued with ID: {nid}")        
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON payload.", err=True)
    except Exception as e:
        click.echo(f"An error occurred: {e}", err=True)

@main_cli.group()
def worker():
    pass

@worker.command()
@click.option('--count', default=1, type=int, help="Number of worker processes to start.")
def start(count):
    wcc = count 
    if not os.path.exists(PIDD):
        try:
            os.makedirs(os.path.dirname(PIDD), exist_ok=True)
        except OSError as e:
            if not os.path.isdir(PIDD):
                click.echo(f"Error: Could not create PID directory at {PIDD}", err=True)
                return
    expids = getRPids()
    splist = []                                
    
    for _ in range(wcc):
        proce = Process(target=startWP, daemon=False)
        proce.start()
        splist.append(proce.pid)
        click.echo(f"Started worker with PID: {proce.pid}")

    saveRPids(expids + splist)

@worker.command()
def stop():
    p2s = getRPids()
    if not p2s:
        click.echo("No workers running.")
        return

    for p2k in p2s:
        try:
            os.kill(p2k, signal.SIGTERM)
            click.echo(f"Sent shutdown signal to worker PID: {p2k}")
        except ProcessLookupError:
            click.echo(f"Worker PID: {p2k} not found (already stopped).")
        except Exception as e:
            click.echo(f"Could not stop PID {p2k}: {e}", err=True)
    
    saveRPids([])
    click.echo("All workers signaled to stop.")

@main_cli.command()
def status():
    led = TaskLedger()
    summ = led.get_status_summary()  
    click.echo("--- Job Status Summary ---")          
    total = 0
    for state, count in summ.items():
        click.echo(f"{state.title()}:\t{count}")
        total += count
    click.echo("--------------------------")
    click.echo(f"Total Jobs:\t{total}")
    
    click.echo("\n--- Worker Status ---")
    pids = getRPids()
    activePids = []
    
    if not pids:
        click.echo("No active workers.")
    else:
        click.echo(f"{len(pids)} active worker(s):")
        for pid in pids:
            try:
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}"],
                    capture_output=True, text=True, check=True
                )                
                if f"{pid}" in result.stdout:
                    click.echo(f"  - PID {pid} (Running)")
                    activePids.append(pid)
                else:
                    click.echo(f"  - PID {pid} (Stale, not running)")            
            except (subprocess.CalledProcessError, FileNotFoundError):
                click.echo(f"  - PID {pid} (Stale, not running)")
    if len(activePids) != len(pids):
        saveRPids(activePids)

@main_cli.command()
@click.option('--state', type=click.Choice([STATUS_PENDING, STATUS_PROCESSING, STATUS_COMPLETED, STATUS_FAILED, STATUS_DEAD]), required=True, help="Job state to filter by.")
def list(state):
    ledger = TaskLedger()
    tasks = ledger.find_tasks_by_status(state)
    
    if not tasks:
        click.echo(f"No jobs found with state: {state}")
        return

    click.echo(f"--- Jobs in '{state}' state ---")
    for task in tasks:
        click.echo(f"ID: {task['id']} | Command: {task['command']} | Attempts: {task['execution_count']}")

@main_cli.group()
def dlq():                                          
    pass

@dlq.command(name="list")
def dlq_list():
    ledger = TaskLedger()
    tasks = ledger.find_tasks_by_status(STATUS_DEAD)
    
    if not tasks:
        click.echo("Dead Letter Queue is empty.")
        return

    click.echo("--- Dead Letter Queue Jobs ---")
    for task in tasks:
        click.echo(f"ID: {task['id']} | Command: {task['command']} | Attempts: {task['execution_count']}")

@dlq.command(name="retry")
@click.argument('job_id', type=str)
def dlq_retry(job_id):
    retryId = job_id 
    ledger = TaskLedger()
    success = ledger.retry_dead_task(retryId)
    
    if success:
        click.echo(f"Job {retryId} has been moved to 'pending' for retry.")
    else:
        click.echo(f"Error: Job {retryId} not found in DLQ.", err=True)

@main_cli.group()
def config():
    pass

@config.command(name="set")
@click.argument('key', type=click.Choice([KEY_RETRY_LIMIT, KEY_BACKOFF_BASE]))
@click.argument('value', type=int)
def config_set(key, value):
    if key == KEY_RETRY_LIMIT and value < 1:
        click.echo("Error: max-retries must be at least 1.", err=True)
        return
    if key == KEY_BACKOFF_BASE and value < 2:
        click.echo("Error: backoff-base must be at least 2 for exponential.", err=True)
        return
        
    ledger = TaskLedger()
    ledger.set_config_value(key, value)
    click.echo(f"Configuration updated: {key} = {value}")

@config.command(name="show")                   
def config_show():
    ledger = TaskLedger()
    retries = ledger.get_config_value(KEY_RETRY_LIMIT)
    base = ledger.get_config_value(KEY_BACKOFF_BASE)
    click.echo("--- Current Configuration ---")
    click.echo(f"{KEY_RETRY_LIMIT}: {retries}")
    click.echo(f"{KEY_BACKOFF_BASE}: {base}")