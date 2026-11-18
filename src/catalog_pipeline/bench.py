"""Optimization experiments (M7): `python -m catalog_pipeline.bench` writes docs/RESULTS.md.

E2: full pipeline runs with different shuffle-partition / broadcast settings, each in a fresh process
against its own lake under `bench.root` (D38, D39). E1: brand-only vs final blocking on one variant's
lake (D41). The real lake (`lake.root`) is never touched.
"""
import argparse
import copy
import functools
import importlib.metadata
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from catalog_pipeline import pipeline
from catalog_pipeline.config import load_config
from catalog_pipeline.ingest.bronze import SUCCESS, bronze_path
from catalog_pipeline.ingest.contracts import load_contracts
from catalog_pipeline.match import candidates, evaluate
from catalog_pipeline.paths import offer_history_path, resolve, silver_table_path
from catalog_pipeline.spark import get_spark
from catalog_pipeline.transform import silver as silver_tf

JOIN_OPERATORS = r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|BroadcastNestedLoopJoin)"


def variant_config(config: dict, name: str) -> dict:
    """The pipeline config for one bench variant: own lake root, its shuffle partitions and broadcast
    setting. Without broadcast, Spark's automatic broadcast of small tables is switched off too (D40)."""
    bench = config["bench"]
    variant = bench["variants"][name]
    cfg = copy.deepcopy(config)
    cfg["lake"]["root"] = str(resolve(bench["root"]) / name / "lake")
    cfg["spark"]["warehouse_dir"] = str(resolve(bench["root"]) / name / "spark-warehouse")
    cfg["spark"]["shuffle_partitions"] = variant["shuffle_partitions"]
    cfg["spark"]["ui_enabled"] = False
    cfg["silver"]["broadcast_aliases"] = variant["broadcast"]
    if not variant["broadcast"]:
        # The threshold (default 10 MB) below which Spark broadcasts a join side on its own; -1 = never.
        cfg["spark"]["extra_conf"] = {**(cfg["spark"].get("extra_conf") or {}), "spark.sql.autoBroadcastJoinThreshold": -1}
    return cfg


def cross_vendor_pairs(df: DataFrame, group: list[str]) -> int:
    """Cross-vendor pairs inside groups, without building them: a group of n records with c_v per
    vendor holds (n^2 - sum(c_v^2)) / 2 pairs whose two records come from different vendors."""
    per_vendor = df.groupBy(*group, "vendor_id").agg(F.count(F.lit(1)).alias("c"))
    per_group = per_vendor.groupBy(*group).agg(F.sum("c").alias("n"), F.sum(F.col("c") * F.col("c")).alias("sq"))
    row = per_group.agg(F.sum((F.col("n") * F.col("n") - F.col("sq")) / 2)).first()  # one scalar
    return int(row[0] or 0)


def brand_only_keys(records: DataFrame) -> DataFrame:
    """E1 baseline blocking: every record of a brand lands in one block (block_token is empty)."""
    return records.filter(F.col("brand_key").isNotNull()).select(
        "record_key", "vendor_id", "brand_key", F.lit("").alias("block_token")
    )


def join_operators(df: DataFrame, marker: str) -> list[str]:
    """Join operators in `df`'s physical plan whose line mentions `marker` (e.g. a join key column)."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    return sorted({m.group(1) for line in plan.splitlines() if marker in line for m in [re.search(JOIN_OPERATORS, line)] if m})


def _alias_join_plan(spark: SparkSession, config: dict) -> list[str]:
    """How the silver brand-alias join is executed in this variant (checked on vendor A's bronze)."""
    contract = load_contracts(resolve(config["ingest"]["contracts_dir"]))["A"]
    bronze = spark.read.format("delta").load(str(bronze_path(contract, config)))
    classified = silver_tf.classify(bronze, pipeline.load_brand_aliases(spark, config), config)
    return join_operators(classified, "_brand_key")


def run_variant(name: str, out_path: str) -> None:
    """One E2 variant from an empty lake: batches, ER, reports. Writes the timings as JSON."""
    config = variant_config(load_config(), name)
    shutil.rmtree(resolve(config["lake"]["root"]).parent, ignore_errors=True)
    spark = get_spark(config)
    spark.sparkContext.setLogLevel("WARN")
    try:
        stages = {}
        start = time.perf_counter()
        results = pipeline.run_all(spark, config)
        rows_in = sum(r.bronze.rows or 0 for r in results if r.bronze.outcome == SUCCESS)
        stages["batches"] = {"seconds": time.perf_counter() - start, "rows": rows_in}

        start = time.perf_counter()
        er = pipeline.resolve_entities(spark, config, force=True)
        stages["er"] = {"seconds": time.perf_counter() - start, "rows": er.metrics["records"]}

        start = time.perf_counter()
        pipeline.build_reports(spark, config)
        history_rows = spark.read.format("delta").load(str(offer_history_path(config))).count()
        stages["reports"] = {"seconds": time.perf_counter() - start, "rows": history_rows}

        result = {
            "variant": name,
            "settings": config["bench"]["variants"][name],
            "stages": stages,
            "total_seconds": sum(s["seconds"] for s in stages.values()),
            "alias_join": _alias_join_plan(spark, config),
            "er": {k: er.metrics[k] for k in ("precision", "recall", "masters", "candidate_pairs", "edges")},
        }
        if name == config["bench"]["blocking_variant"]:
            result["blocking"] = blocking_experiment(spark, config)
            result["machine"] = machine_specs(spark)
    finally:
        spark.stop()
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)


def _blocking_stats(keys: DataFrame, max_block_size: int) -> tuple[DataFrame, dict]:
    pairs, dropped = candidates.block_pairs(keys, max_block_size)
    pairs = pairs.localCheckpoint()  # computes the pairs once; counted and evaluated below
    stats = {
        "blocks": keys.select("brand_key", "block_token").distinct().count(),
        "blocks_dropped": dropped.count(),
        "uncapped_pairs": cross_vendor_pairs(keys.dropDuplicates(["record_key", "brand_key", "block_token"]), ["brand_key", "block_token"]),
    }
    return pairs, stats


def blocking_experiment(spark: SparkSession, config: dict) -> dict:
    """E1 (D41): brand-only vs final blocking on the variant's silver data. Candidate pairs, blocks,
    pair recall (with and without the GTIN pairs) and time to build the candidates."""
    er = config["er"]
    blocking = er["blocking"]
    products = spark.read.format("delta").load(str(silver_table_path("vendor_product", config)))
    prices = spark.read.format("delta").load(str(offer_history_path(config))).filter("is_current").select("offer_key", "price")
    records = candidates.prepare(products, prices).localCheckpoint()
    gtin = candidates.gtin_pairs(records).localCheckpoint()
    truth = pipeline.load_ground_truth(spark, config).join(records.select("record_key", "vendor_id"), "record_key")
    true_pairs = cross_vendor_pairs(truth, ["true_product_id"])

    def evaluate_pairs(pairs: DataFrame, stats: dict, seconds: float) -> dict:
        n = pairs.count()
        found = evaluate.edge_metrics(pairs, truth)["correct_edges"]
        with_gtin = evaluate.edge_metrics(pairs.unionByName(gtin).distinct(), truth)["correct_edges"]
        return {**stats, "candidate_pairs": n, "seconds": seconds, "true_pairs_found": found,
                "pair_recall": found / true_pairs, "pair_recall_with_gtin": with_gtin / true_pairs}

    out = {"true_pairs": true_pairs, "gtin_pairs": gtin.count(), "max_block_size": blocking["max_block_size"]}

    start = time.perf_counter()
    pairs, stats = _blocking_stats(brand_only_keys(records), blocking["max_block_size"])
    out["brand_only"] = evaluate_pairs(pairs, stats, time.perf_counter() - start)

    start = time.perf_counter()
    passes = [_blocking_stats(keys, blocking["max_block_size"]) for keys in candidates.block_key_passes(records, blocking)]
    pairs = functools.reduce(DataFrame.unionByName, [p for p, _ in passes]).distinct().localCheckpoint()
    # uncapped_pairs adds up the passes, so a pair found by several is counted more than once (an upper bound).
    stats = {k: sum(s[k] for _, s in passes) for k in passes[0][1]}
    out["final"] = evaluate_pairs(pairs, stats, time.perf_counter() - start)
    return out


def machine_specs(spark: SparkSession) -> dict:
    """CPU, cores, RAM, OS and library versions of the machine the bench ran on."""
    def proc(path: str, key: str) -> str | None:
        try:
            with open(path) as f:
                return next((line.split(":", 1)[1].strip() for line in f if line.startswith(key)), None)
        except OSError:
            return None

    mem_kb = proc("/proc/meminfo", "MemTotal")
    return {
        "cpu": proc("/proc/cpuinfo", "model name") or platform.processor(),
        "cores": os.cpu_count(),
        "ram_gb": round(int(mem_kb.split()[0]) / 1024**2, 1) if mem_kb else None,
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "java": spark.sparkContext._jvm.System.getProperty("java.version"),
        "spark": spark.version,
        "delta": importlib.metadata.version("delta-spark"),
        "spark_master": spark.sparkContext.master,
        "driver_memory": spark.conf.get("spark.driver.memory"),
    }


def _run_subprocess(name: str, out_path: str) -> dict:
    subprocess.run([sys.executable, "-m", "catalog_pipeline.bench", "variant", name, "--out", out_path], check=True)
    with open(out_path) as f:
        return json.load(f)


def render(results: dict[str, dict], repeats: int) -> str:
    """docs/RESULTS.md from the per-variant results (the fastest repeat of each)."""
    blocking_run = next(r for r in results.values() if "blocking" in r)
    m, b = blocking_run["machine"], blocking_run["blocking"]
    secs = lambda s: f"{s:,.1f}"  # noqa: E731
    lines = [
        "# Results",
        "",
        f"Generated by `make bench` on {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC. Each variant ran in a fresh "
        f"process against its own empty lake; best of {repeats} run(s) per variant (D38). Do not edit by hand.",
        "",
        "## Machine",
        "",
        "| | |", "|---|---|",
        *[f"| {k} | {v} |" for k, v in m.items()],
        "",
        "## E2: full run, shuffle partitions and broadcast",
        "",
        "Stage times in seconds. `batches` = bronze + silver + SCD2 history for all landed batches. AQE is on in "
        "every variant and coalesces small shuffle partitions at runtime, which narrows the 200 vs 8 gap (D39). "
        "No broadcast = alias-join hint off and `spark.sql.autoBroadcastJoinThreshold=-1` (D40).",
        "",
        "| variant | shuffle partitions | broadcast | batches | er | reports | total | alias join | ER precision | ER recall |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for name, r in results.items():
        st, s = r["stages"], r["settings"]
        lines.append(
            f"| {name} | {s['shuffle_partitions']} | {'on' if s['broadcast'] else 'off'} | {secs(st['batches']['seconds'])} | "
            f"{secs(st['er']['seconds'])} | {secs(st['reports']['seconds'])} | **{secs(r['total_seconds'])}** | "
            f"{', '.join(r['alias_join']) or '-'} | {r['er']['precision']:.4f} | {r['er']['recall']:.4f} |"
        )
    lines += [
        "",
        f"## Rows per stage and rows/sec ({blocking_run['variant']} variant)",
        "",
        "| stage | rows | rows counted | seconds | rows/sec |",
        "|---|---|---|---|---|",
    ]
    meaning = {"batches": "landed feed rows read", "er": "ER input records", "reports": "offer_history rows"}
    for stage, st in blocking_run["stages"].items():
        lines.append(f"| {stage} | {st['rows']:,} | {meaning[stage]} | {secs(st['seconds'])} | {st['rows'] / st['seconds']:,.0f} |")
    e = blocking_run["er"]
    lines += [
        "",
        f"ER: precision **{e['precision']:.4f}**, recall **{e['recall']:.4f}**, {e['masters']:,} masters, "
        f"{e['candidate_pairs']:,} candidate pairs, {e['edges']:,} match edges.",
        "",
        "## E1: blocking, brand-only vs final",
        "",
        f"Same max block size ({b['max_block_size']}) for both. Pair recall = share of the {b['true_pairs']:,} true "
        f"cross-vendor pairs (ground truth) that are candidates; 'with GTIN' adds the {b['gtin_pairs']:,} exact GTIN pairs. "
        "Uncapped pairs = cross-vendor pairs the blocks would produce without the size cap (final: both passes "
        "added, an upper bound). Seconds = building and counting the candidates (D41).",
        "",
        "| blocking | blocks | blocks dropped | uncapped pairs | candidate pairs | pair recall | pair recall with GTIN | seconds |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for name, label in (("brand_only", "brand only"), ("final", "brand + model token, brand + title prefix")):
        x = b[name]
        lines.append(
            f"| {label} | {x['blocks']:,} | {x['blocks_dropped']:,} | {x['uncapped_pairs']:,} | {x['candidate_pairs']:,} | "
            f"{x['pair_recall']:.4f} | {x['pair_recall_with_gtin']:.4f} | {secs(x['seconds'])} |"
        )
    lines += [
        "",
        "## Not measured",
        "",
        "OPTIMIZE / ZORDER on `gold.offer_history` (D15) and `spark.databricks.delta.snapshotPartitions` (D9) (D43).",
        "",
    ]
    return "\n".join(lines)


def run_bench(config: dict) -> None:
    bench = config["bench"]
    root = resolve(bench["root"])
    root.mkdir(parents=True, exist_ok=True)
    best = {}
    for name in bench["variants"]:
        for i in range(bench["repeats"]):
            print(f"=== bench variant {name} (run {i + 1}/{bench['repeats']}) ===", flush=True)
            r = _run_subprocess(name, str(root / f"{name}.json"))
            if name not in best or r["total_seconds"] < best[name]["total_seconds"]:
                best[name] = r
    (root / "results.json").write_text(json.dumps(best, indent=2))
    resolve(bench["results_path"]).write_text(render(best, bench["repeats"]))
    print(f"wrote {resolve(bench['results_path'])}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="catalog_pipeline.bench")
    parser.add_argument("command", nargs="?", default="all", choices=["all", "variant", "render"])
    parser.add_argument("name", nargs="?", help="variant: which variant to run")
    parser.add_argument("--out", help="variant: where to write its JSON result")
    args = parser.parse_args(argv)
    config = load_config()
    if args.command == "variant":
        run_variant(args.name, args.out)
    elif args.command == "render":  # re-render RESULTS.md from data/bench/results.json
        results = json.loads((resolve(config["bench"]["root"]) / "results.json").read_text())
        resolve(config["bench"]["results_path"]).write_text(render(results, config["bench"]["repeats"]))
    else:
        run_bench(config)


if __name__ == "__main__":
    main()
