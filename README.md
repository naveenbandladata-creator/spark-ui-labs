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

Each lab file starts with a **Your turn** section. Exercise episodes (6, 12, 18, 24) have answers in `answers/`.
Some labs write a small Parquet table under `/tmp/spark-ui-labs/`. Try first.

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
| 7 | ep07_summary_metrics.py | one task reads ~31x the median records |
| 8 | ep08_task_timeline.py | 400 tiny tasks ~6x slower than 4 normal ones |
| 9 | ep09_shuffle_read_write.py | ~105 MB shuffled vs a few KB for the same answer |
| 10 | ep10_spill.py | ~420 MB spilled with 2 partitions, 0 with 32 |
| 11 | ep11_executors.py | a failed-then-retried task, a dead executor, a replacement |
| 12 | ep12_spot_the_problem_2.py | exercise: even record counts hiding 26x bigger data |
| 13 | ep13_sql_follow_the_rows.py | a join turning 1M rows into 3M; dropDuplicates shows up as HashAggregate |
| 14 | ep14_which_join.py | BroadcastHashJoin (1 Exchange) vs SortMergeJoin (3), ~3.5 s vs ~8 s |
| 15 | ep15_scan_node.py | 1 of 12 month folders read; 24 MB vs 190 MB read from the same files |
| 16 | ep16_aqe.py | 200 tasks -> 1 after AQE; sort-merge join switched to broadcast at runtime |
| 17 | ep17_storage_cache.py | a cache that never filled, 6 of 16 partitions cached, a 5 s -> 1 s second use |
| 18 | ep18_spot_the_problem_3.py | exercise: all 12 folders read, and a join that makes revenue 4x too big |
