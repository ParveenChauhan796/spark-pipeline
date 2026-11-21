"""Pipeline CLI: `python -m catalog_pipeline.pipeline {run-all,er,reports,show}`."""
import argparse

from catalog_pipeline.config import load_config
from catalog_pipeline.pipeline.er import resolve_entities
from catalog_pipeline.pipeline.outcomes import ErOutcome
from catalog_pipeline.pipeline.reports import build_reports, report_previews
from catalog_pipeline.pipeline.run import run_all
from catalog_pipeline.spark import get_spark


def _describe_er(outcome: ErOutcome) -> str:
    m = outcome.metrics
    if not m:
        return f"er {outcome.status}"
    pct = lambda v: "n/a" if v is None else f"{v:.4f}"  # noqa: E731
    return (
        f"er {outcome.status}: records={m['records']} candidates={m['candidate_pairs']} (pass1 {m['candidate_pairs_pass1']}) "
        f"gtin={m['gtin_pairs']} fuzzy={m['fuzzy_edges']} masters={m['masters']} "
        f"precision={pct(m['precision'])} recall={pct(m['recall'])} edge_precision={pct(m['edge_precision'])} "
        f"same_vendor_clusters={m['same_vendor_clusters']} [{m['seconds']}s]"
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="catalog_pipeline.pipeline")
    parser.add_argument("command", choices=["run-all", "er", "reports", "show"])
    parser.add_argument("--force", action="store_true", help="er: re-run even if the inputs did not change")
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark(config)
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.command == "run-all":
            run_all(spark, config)
        if args.command in ("run-all", "er"):
            print(_describe_er(resolve_entities(spark, config, force=args.force)))
        if args.command in ("run-all", "reports"):
            counts = build_reports(spark, config)
            print("reports NOT_READY (run er first)" if counts is None else f"reports DONE: {counts}")
        if args.command == "show":
            previews = report_previews(spark, config)
            if previews is None:
                print("no reports yet (run `make run-all` first)")
            for name, df in (previews or {}).items():
                print(f"\n== gold.{config['reports']['tables'][name]} ==")
                df.show(config["reports"]["show"][name]["n"], truncate=False)
    finally:
        spark.stop()

