import logging
import subprocess
import os
import sys
import time
import threading
import json
import re
import urllib.request
from prefect import flow, task, serve
from prometheus_client import start_http_server, Counter, Gauge
import redis
import psycopg2
from kafka import KafkaProducer

sys.path.append("/app/ml_pipeline")
from batch_predictor import run_one_cycle, KAFKA_BOOTSTRAP, POSTGRES_DSN, DRAGONFLY_HOST, DRAGONFLY_PASSWORD, get_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("training_flow")

try:
    start_http_server(8002)
except OSError:
    log.info("Prometheus port 8002 already in use (Prefect subprocess)")

BATCH_CYCLES = Counter("batch_predictor_cycles_total", "Completed cycles")
BATCH_ERRORS = Counter("batch_predictor_errors_total", "Total errors")
BATCH_LAST = Gauge("batch_predictor_last_success_seconds", "Last success timestamp")
BATCH_PRED_VOL = Gauge("batch_predictor_predicted_vol", "Latest predicted volatility")
BATCH_ACT_VOL = Gauge("batch_predictor_actual_vol", "Latest actual volatility")
DBT_RUNS = Counter("dbt_runs_total", "Total dbt test runs")
DBT_FAILURES = Counter("dbt_test_failures_total", "Cumulative dbt test failures")
DBT_LAST_PASS = Gauge("dbt_last_pass_count", "Pass count of last dbt run")
DBT_LAST_ERROR = Gauge("dbt_last_error_count", "Error count of last dbt run")
DBT_LAST_SUCCESS = Gauge("dbt_last_success_seconds", "Timestamp of last successful dbt run")

DEPLOYMENT_NAMES = {
    "dxy-training": "dxy-training",
    "dxy-batch-predict": "batch-predict",
    "dbt-test": "dbt-test",
}


def _api_get(path):
    req = urllib.request.Request(f"http://prefect-server:4200{path}", method="GET", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode())


def _api_patch(path, data):
    body = json.dumps(data).encode()
    req = urllib.request.Request(f"http://prefect-server:4200{path}", data=body, method="PATCH", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode())


def fix_deployment_paths():
    time.sleep(15)
    for name, flow_name in DEPLOYMENT_NAMES.items():
        try:
            for attempt in range(5):
                try:
                    dep = _api_get(f"/api/deployments/name/{flow_name}/{name}")
                    break
                except Exception:
                    if attempt == 4:
                        raise
                    time.sleep(3)
            dep_id = dep["id"]
            if dep.get("path") != "/app":
                _api_patch(f"/api/deployments/{dep_id}", {"path": "/app"})
                log.info("Deployment %s path set to /app", name)
        except Exception as e:
            log.warning("Failed to fix path for %s: %s", name, e)


@task(retries=2, retry_delay_seconds=30)
def run_training():
    result = subprocess.run(
        ["python", "/app/ml_pipeline/training.py"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        log.error("Training failed: %s", result.stderr)
        raise RuntimeError(result.stderr)
    log.info("Training output:\n%s", result.stdout)
    return result.stdout


@flow(log_prints=True)
def dxy_training():
    run_training()


@task(retries=2, retry_delay_seconds=10)
def run_batch_cycle():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
        acks=1,
    )
    conn, cur = get_db()
    r = redis.Redis(host=DRAGONFLY_HOST, port=6379, password=DRAGONFLY_PASSWORD, decode_responses=True)
    run_one_cycle(producer, conn, cur, r)
    conn.close()


@flow(log_prints=True)
def batch_predict():
    try:
        run_batch_cycle()
    except Exception as e:
        log.error("Batch predict failed: %s", e)

    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batch_metrics (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            predicted_vol DOUBLE PRECISION,
            actual_vol DOUBLE PRECISION,
            success BOOLEAN,
            error_message TEXT
        )
    """)
    r = redis.Redis(host=DRAGONFLY_HOST, port=6379, password=DRAGONFLY_PASSWORD, decode_responses=True)
    predicted_vol = r.get("latest:dxy:batch_predicted_volatility")
    actual_vol = r.get("latest:dxy:instant_volatility")
    cur.execute(
        "INSERT INTO batch_metrics (predicted_vol, actual_vol, success, error_message) VALUES (%s, %s, %s, %s)",
        (float(predicted_vol) if predicted_vol else None,
         float(actual_vol) if actual_vol else None,
         True, None)
    )
    conn.commit()
    cur.close()
    conn.close()


@task(retries=1, retry_delay_seconds=30)
def run_dbt_test():
    result = subprocess.run(
        ["dbt", "test", "--store-failures", "--profiles-dir", "/dbt/profiles"],
        cwd="/dbt",
        capture_output=True,
        text=True,
        timeout=120,
    )
    output = result.stdout + result.stderr
    log.info("dbt test output:\n%s", output)

    passed = 0
    warnings = 0
    errors = 0
    skipped = 0
    total = 0
    models = 0
    duration = 0.0

    m = re.search(r"Found (\d+) models", output)
    if m:
        models = int(m.group(1))

    m = re.search(r"PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+).*TOTAL=(\d+)", output)
    if m:
        passed = int(m.group(1))
        warnings = int(m.group(2))
        errors = int(m.group(3))
        skipped = int(m.group(4))
        total = int(m.group(5))

    m = re.search(r"in ([\d.]+) seconds", output)
    if m:
        duration = float(m.group(1))

    if result.returncode != 0:
        log.warning("dbt test found failures: %d errors, %d warnings", errors, warnings)

    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dbt_test_results (
            id SERIAL PRIMARY KEY,
            run_at TIMESTAMPTZ DEFAULT NOW(),
            models_found INTEGER,
            tests_total INTEGER,
            passed INTEGER,
            warnings INTEGER,
            errors INTEGER,
            skipped INTEGER,
            duration_seconds DOUBLE PRECISION,
            success BOOLEAN,
            raw_output TEXT
        )
    """)
    cur.execute("""
        INSERT INTO dbt_test_results (models_found, tests_total, passed, warnings, errors, skipped, duration_seconds, success, raw_output)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (models, total, passed, warnings, errors, skipped, duration, result.returncode == 0, output))
    conn.commit()
    cur.close()
    conn.close()

    DBT_RUNS.inc()
    DBT_FAILURES.inc(errors)
    DBT_LAST_PASS.set(passed)
    DBT_LAST_ERROR.set(errors)
    if result.returncode == 0:
        DBT_LAST_SUCCESS.set(time.time())

    if result.returncode != 0:
        raise RuntimeError(f"dbt test failed: {errors} errors, {warnings} warnings")

    return {"passed": passed, "warnings": warnings, "errors": errors, "total": total}


@flow(log_prints=True)
def dbt_test():
    run_dbt_test()


if __name__ == "__main__":
    threading.Thread(target=fix_deployment_paths, daemon=True).start()
    serve(
        dxy_training.to_deployment(name="dxy-training", interval=3600, work_pool_name="default"),
        batch_predict.to_deployment(name="dxy-batch-predict", interval=60, work_pool_name="default"),
        dbt_test.to_deployment(name="dbt-test", interval=3600, work_pool_name="default"),
    )
