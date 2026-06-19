import logging
import subprocess
from prefect import flow, task

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("training_flow")


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


if __name__ == "__main__":
    dxy_training.serve(name="dxy-training-manual")
