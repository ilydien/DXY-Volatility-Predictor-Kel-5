import logging
import os
import psycopg2
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("data_quality")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://gold:gold@postgres:5432/golddb")


def run_checks():
    conn = psycopg2.connect(POSTGRES_DSN)
    results = []

    tables = [
        {"name": "market_data", "expected_types": {"open": "double", "close": "double", "volume": "bigint"}},
        {"name": "intraday_bars", "expected_types": {"open": "double", "close": "double", "volume": "bigint"}},
        {"name": "predictions", "expected_types": {"predicted_close": "double", "actual_close": "double"}},
    ]

    for table in tables:
        name = table["name"]
        log.info("--- Checking %s ---", name)

        count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {name}", conn)["n"].iloc[0]
        log.info("  Row count: %d", count)
        results.append({"table": name, "check": "row_count", "status": "PASS", "detail": str(count)})

        for col, expected_type in table["expected_types"].items():
            col_type = pd.read_sql(
                f"SELECT data_type FROM information_schema.columns WHERE table_name='{name}' AND column_name='{col}'",
                conn,
            )
            if col_type.empty:
                log.warning("  Column %s.%s: NOT FOUND", name, col)
                results.append({"table": name, "check": f"column_exists:{col}", "status": "FAIL", "detail": "not found"})
                continue
            actual = col_type["data_type"].iloc[0]
            ok = expected_type in actual
            log.log(logging.INFO if ok else logging.ERROR, "  Column %s.%s: type=%s (expected %s) %s", name, col, actual, expected_type, "PASS" if ok else "FAIL")
            results.append({"table": name, "check": f"column_type:{col}", "status": "PASS" if ok else "FAIL", "detail": f"{actual} vs {expected_type}"})

        null_checks = []
        cur = conn.cursor()
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{name}' AND is_nullable='YES'")
        nullable_cols = [r[0] for r in cur.fetchall()]
        for col in nullable_cols:
            null_count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {name} WHERE {col} IS NULL", conn)["n"].iloc[0]
            if null_count > 0:
                log.warning("  Column %s.%s: %d NULL values", name, col, null_count)
                null_checks.append({"col": col, "nulls": null_count})
        if null_checks:
            for nc in null_checks:
                results.append({"table": name, "check": f"null_check:{nc['col']}", "status": "WARN", "detail": f"{nc['nulls']} nulls"})
        else:
            results.append({"table": name, "check": "null_check", "status": "PASS", "detail": "no nulls in nullable columns"})

        cur.close()

    conn.close()
    log.info("=== Data Quality Summary ===")
    fails = [r for r in results if r["status"] == "FAIL"]
    warns = [r for r in results if r["status"] == "WARN"]
    log.info("Total checks: %d | PASS: %d | WARN: %d | FAIL: %d", len(results), len(results) - len(fails) - len(warns), len(warns), len(fails))
    for f in fails:
        log.error("  FAIL: %s | %s | %s", f["table"], f["check"], f["detail"])
    for w in warns:
        log.warning("  WARN: %s | %s | %s", w["table"], w["check"], w["detail"])

    return results


if __name__ == "__main__":
    run_checks()
