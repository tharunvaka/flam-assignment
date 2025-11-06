# QueueCTL - A Simple CLI Job Queue
# Done By Vaka Tharun Kumar

This is a command-line job queue system I built in Python.

It manages background jobs using a SQLite database, so all jobs are saved. It can run multiple workers at the same time, and it automatically retries failed jobs. If a job fails too many times, it goes to a Dead Letter Queue (DLQ).

## How to Setup and Run

First, you need to get the code.
```bash
git clone [https://github.com/tharunvaka/flam-assignment.git](https://github.com/tharunvaka/flam-assignment.git)
cd flam-assignment

## My Demo Video

You can watch a full demo of the project here:
[https://drive.google.com/file/d/1qBOtIqWrpekBvLJ9oUmKMdlGe9Sj-e3O/view?usp=sharing]