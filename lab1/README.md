# Lab 1: Customers into Iceberg

This pipeline has two datasets. The first appends new CSV files. The second reads that table and writes age-group totals.

```text
data/raw/*.csv
        |
        v
dev.lab1.raw_customers            streaming append, checkpointed
        |
        v
dev.lab1.customers_by_age_group   materialized view, recounts every run
```

`raw_customers` runs first. `customers_by_age_group` waits for it. The raw table appends. The summary replaces its rows with the current totals, because a grouped count has to be recalculated from every raw row.

Docs: [Spark Declarative Pipelines](https://spark.apache.org/docs/4.1.1/declarative-pipelines-programming-guide.html), [Iceberg with Spark](https://iceberg.apache.org/docs/latest/spark-configuration/).

## How a pipeline is put together

A **pipeline project** is this folder. `spark-pipeline.yml` names the pipeline, says where the code is (`transformations/`), which catalog and database receive tables (`dev.lab1`), and where checkpoints go (`pipeline-storage/`).

A **dataset** is something the pipeline can query:

| Kind | In this lab | Saved? |
| --- | --- | --- |
| Streaming table | `raw_customers` | Yes. Appends new CSV rows. The checkpoint remembers which files were read. |
| Materialized view | `customers_by_age_group` | Yes. Every run replaces this table with the current age-group totals. |
| Temporary view | not used here | No. Useful when several later steps share the same read. |
| Sink | not used here | A streaming write to a system outside the catalog, such as Kafka. |

A **flow** is the function under each decorator. It reads something, applies your transformation, and returns a DataFrame. Spark writes that DataFrame to the dataset. You do not call `save` or `saveAsTable` yourself.

The **graph** is those datasets and the links between them. `customers_by_age_group` reads `raw_customers`, so that edge is the graph. A dry run checks it. A real run executes `raw_customers`, then `customers_by_age_group`. The CLI has no separate `dag` subcommand. The run log is the graph executing: each flow goes `QUEUED`, `STARTING`, `RUNNING`, `COMPLETED`.

## What the transformation does

Each CSV has `name`, `cust_id`, `age`, `city`, and `last_updated`, all stored as text. `data/raw/batch1.csv` is the first batch. `data/next/batch2.csv` stays out of that folder until you want a second batch.

`customers_by_age_group` is written in SQL. It reads `raw_customers`, turns `age` into `under_30`, `30_to_59`, or `60_plus`, and writes one row per group with `customer_count`, `avg_age`, `youngest`, and `oldest`.

## Where Iceberg is configured

`conf/spark-defaults.conf` holds settings Spark reads at startup:

- the jar `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0`
- the `dev` catalog, type `hadoop`
- the warehouse `lab1/warehouse`

Iceberg 1.12 publishes a Spark 4.1 runtime, so this lab uses PySpark 4.1.1. Those startup settings live in `conf/spark-defaults.conf`, loaded through `SPARK_CONF_DIR`. Putting the jar or `spark.sql.extensions` only in `spark-pipeline.yml` is too late: Spark has already started, and the run fails with `CANNOT_MODIFY_STATIC_CONFIG`.

`spark-pipeline.yml` then points the pipeline at that catalog (`catalog: dev`, `database: lab1`). In this Hadoop catalog, the schema `lab1` is the folder `warehouse/lab1`. That folder has to exist before `spark-pipelines` starts. It is already in the project. If you delete it, dry-run fails with `SCHEMA_NOT_FOUND` for `dev.lab1`.

## What you need

- Java 17 (`java -version`). This machine uses Homebrew Java 17 at `/opt/homebrew/opt/openjdk@17`.
- Python 3.10 or newer.

## Install once

From this directory (`lab1/`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export SPARK_CONF_DIR="$PWD/conf"
```

Keep `SPARK_CONF_DIR` set for every command below. The first `spark-pipelines` command downloads the Iceberg jar from Maven.

If this folder already has a `.venv` from PySpark 4.2, remove it and create the venv again.

## Commands

Run these from `lab1/` with the virtualenv active and `SPARK_CONF_DIR` set. The CLI has three subcommands: `dry-run`, `run`, and `init`.

### 1. Dry run — check the graph

```bash
spark-pipelines dry-run
```

Same check, naming the spec file:

```bash
spark-pipelines dry-run --spec spark-pipeline.yml
```

A dry run loads the spec, imports `transformations/customers.py`, registers `transformations/customers_by_age_group.sql`, and checks the graph. It catches a bad Python or SQL file, a missing table or column, and a cycle. The CSV and the warehouse stay as they are. Run this first.

### 2. Run — read, transform, write Iceberg

```bash
spark-pipelines run
```

Same run, with the spec path written out:

```bash
spark-pipelines run --spec spark-pipeline.yml
```

With no extra flags this is an incremental update. The log shows `raw_customers` complete, then `customers_by_age_group`. The first run reads `data/raw/batch1.csv` and appends Ada and Grace. The summary then has two age groups.

Print the table:

```bash
.venv/bin/python show_customers.py
```

You should see 2 raw rows and 2 summary rows.

### 3. Add a new file and refresh

`--refresh customers_by_age_group` updates that dataset. Spark still runs `raw_customers` first, because the summary reads it. The raw checkpoint skips files it already stored. The summary is counted again from every raw row.

```bash
cp data/next/batch2.csv data/raw/
spark-pipelines run --refresh customers_by_age_group
.venv/bin/python show_customers.py
```

You should see 4 raw rows and 3 summary rows. `30_to_59` has a count of 2. Run `--refresh customers_by_age_group` once more without adding a file and both tables stay the same.

`spark-pipelines run` with no flags does this same incremental update for every dataset.

### 4. Full refresh — forget progress and load every file again

```bash
spark-pipelines run --full-refresh raw_customers,customers_by_age_group
```

This deletes the raw checkpoint and both tables' rows, then reads every CSV currently in `data/raw/` and recounts the age groups. With both batch files present, the raw table has 4 rows again, not 8.

The same reset for every dataset:

```bash
spark-pipelines run --full-refresh-all
```

`--full-refresh-all` stands alone. On a pipeline with more than one dataset, `--refresh` and `--full-refresh` can be combined to treat them differently.

### 5. Scaffold a different project

This lab already exists. `init` starts a new empty pipeline somewhere else:

```bash
spark-pipelines init --name my_pipeline
```

## Files

| File | Role |
| --- | --- |
| `conf/spark-defaults.conf` | Iceberg jar, `dev` catalog, warehouse path |
| `spark-pipeline.yml` | Pipeline name, catalog, database, checkpoint directory, code location |
| `data/raw/batch1.csv` | First batch. The stream reads every CSV in `data/raw/` |
| `data/next/batch2.csv` | Second batch. Copy it into `data/raw/` to see an incremental append |
| `transformations/customers.py` | Bronze streaming table `raw_customers`. Reads new CSV files and checkpoints them. |
| `transformations/customers_by_age_group.sql` | Silver materialized view. Recounts `raw_customers` by age group. |
| `show_customers.py` | Prints `dev.lab1.raw_customers` and `dev.lab1.customers_by_age_group` |
| `warehouse/` | Local Iceberg warehouse. `warehouse/lab1` is the `dev.lab1` schema and must exist before a run |
| `pipeline-storage/` | Checkpoint directory required by the spec |

If you move this project, update the `file://` paths in `conf/spark-defaults.conf`, `spark-pipeline.yml`, and `transformations/customers.py`.
