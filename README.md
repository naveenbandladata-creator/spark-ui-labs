# spark-ui-labs

Companion labs for **Spark UI in Motion** on [Data In Motion](https://www.youtube.com/@DataInMotionYT).
Every episode has a script here that runs the *same* job as the video, so you can open your own Spark UI and find the problem yourself.

## Setup (once, about 5 minutes)
1. Install **Java 17 or 21** (`java -version` to check).
2. `pip install -r requirements.txt`
3. Start a small cluster on your machine (1 master + 2 workers):
   ```bash
   bash start_cluster.sh        # then open http://localhost:8080 - 2 ALIVE workers
   ```
   Windows: run inside WSL.

## Run a lab
```bash
cd labs
python3 ep02_one_action_many_jobs.py
```
Open the link it prints (usually **http://localhost:4040**). The app waits for you to press Enter,
because the Spark UI disappears when the app stops.

Each lab file starts with a **Your turn** section. Exercise episodes (6, 12, 18, 24) have answers in `answers/`. Try first.

Stop the cluster: `bash stop_cluster.sh`

## Why a local cluster and not `local[*]`?
In local mode everything runs in one process, so the Executors tab shows only the driver.
A 2-worker cluster gives you real executors, real shuffles and a task timeline that looks like production.
One honest limit: both workers are on one machine, so "remote" shuffle reads always show 0.

## Versions
Spark **4.0.1**. Settings are fixed in `labs/common.py` so your numbers land close to the video's.
They will not match exactly: timings depend on your machine.

| Ep | Lab | What you'll see |
|---|---|---|
| 1 | ep01_where_is_the_ui.py | the UI, port 4041 for a 2nd app, Environment tab |
| 2 | ep02_one_action_many_jobs.py | one action -> 2-3 jobs; readable job names |
| 3 | ep03_where_stages_split.py | 1, 2 and 4 stages; where shuffles split jobs |
| 4 | ep04_read_the_dag.py | DAG and SQL graph, 3 Exchange boxes |
| 5 | ep05_timeline_gaps.py | idle gaps from driver-side Python and toPandas |
| 6 | ep06_spot_the_problem_1.py | exercise: 40 jobs for one report |
