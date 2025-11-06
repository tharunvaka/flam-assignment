# QueueCTL - A Simple CLI Job Queue
# Done By Vaka Tharun Kumar

This is a command-line job queue system I built in Python.

It manages background jobs using a SQLite database, so all jobs are saved. It can run multiple workers at the same time, and it automatically retries failed jobs. If a job fails too many times, it goes to a Dead Letter Queue (DLQ).

---

## 1. Setup Instructions

First, you need to get the code.
```bash
git clone [https://github.com/tharunvaka/flam-assignment.git](https://github.com/tharunvaka/flam-assignment.git)
cd flam-assignment
```
Now, make a venv for the python packages:
```bash
# Make the venv folder
python -m venv venv

# Activate it (on Windows cmd)
.\venv\Scripts\activate
```

Then, install the tool. The -e makes it "editable."
```bash
pip install -e .
```
Now you can run the queuectl command from your terminal.

## 2. Usage Examples

You need two terminals.

In Terminal 1 (The Worker): Start a worker. It will just sit and wait for jobs.
```bash
queuectl worker start --count 1
```

In Terminal 2 (The Client): Now you can send commands.
```bash
# Set the config (3 retries, 2s base backoff)
queuectl config set CEILING_RETRY_ATTEMPTS 3
queuectl config set RETRY_EXP_BASE 2

# Add a job that works
queuectl enqueue "{\"command\": \"echo Hello World\"}"

# Add a job that will fail
queuectl enqueue "{\"command\": \"exit 1\"}"

# Check the status of all jobs
queuectl status

# List only the failed jobs
queuectl list --state failed

# List jobs that are in the DLQ
queuectl dlq list

# Retry a job from the DLQ
queuectl dlq retry <job-id-here>

# Stop the worker in Terminal 1
queuectl worker stop
```

## 3. Architecture Overview

The code is split into a few files:

* **`persistence.py`**: This file handles all the database stuff. It uses `sqlite3` to save jobs and config.
* **`executor.py`**: This is the code for the `Worker`. It finds new jobs in the database, runs them, and updates their status.
* **`cli.py`**: This file has all the `queuectl` commands. It uses `click` to make the CLI.

### Job Lifecycle

The job lifecycle is simple:

1.  You `enqueue` a job. Its status is `pending`.
2.  A worker finds it and sets its status to `processing`.
3.  If the job's command works (exit code 0), the status becomes `completed`.
4.  If the command fails (exit code not 0), the status becomes `failed`.
5.  The worker will retry the `failed` job.
6.  If it fails too many times, the worker moves it to the DLQ by setting the status to `dead`.


## 4. Assumptions & Trade-offs

* **Database:** I used SQLite because it's built-in and simple, which is perfect for this assignment. A larger system might use PostgreSQL or Redis for more speed.
* **Concurrency:** The system uses file-based-locking from SQLite (`BEGIN IMMEDIATE`) to stop two workers from grabbing the same job. This is simple and 100% safe.
* **Worker Management:** The workers are managed with a PID file. This is simple, but a real production system might use a more advanced tool or service manager.


## 5. Testing Instructions

Here is how you can prove all features are working.

**Test 1: Job completes (the "happy path")**
1.  Start the worker in Terminal 1.
2.  In Terminal 2, run `queuectl enqueue "{\"command\": \"echo test success\"}"`.
3.  Watch Terminal 1. It will print that it processed and completed the job.
4.  In Terminal 2, run `queuectl status`. You will see `Completed: 1`.

**Test 2: Failed job moves to DLQ**
1.  Start the worker in Terminal 1.
2.  In Terminal 2, run `queuectl enqueue "{\"command\": \"exit 1\"}"`.
3.  Watch Terminal 1. It will show the job failing, retrying after 2s, failing again, retrying after 4s, and finally moving to the DLQ.
4.  In Terminal 2, run `queuectl dlq list`. You will see the job here with "Attempts: 3".

**Test 3: Data persists after restart**
1.  Stop all workers (`queuectl worker stop`).
2.  Enqueue a job: `queuectl enqueue "{\"command\": \"echo persistent test\"}"`.
3.  Check status: `queuectl status`. You will see `Pending: 1`.
4.  Now, start a worker: `queuectl worker start --count 1`.
5.  Watch the worker. It will find the old job and process it.
6.  Check status again: `queuectl status`. You will see `Completed: 1`.


---

## My Demo Video

You can watch a full demo of the project here:
[https://drive.google.com/file/d/1qBOtIqWrpekBvLJ9oUmKMdlGe9Sj-e3O/view?usp=sharing](https://drive.google.com/file/d/1qBOtIqWrpekBvLJ9oUmKMdlGe9Sj-e3O/view?usp=sharing)