import argparse
import datetime
import json
import os
import pathlib

import polars as pl
import pytz


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--input-json", "-i", required=True)
    arg_parser.add_argument("--output-dir", "-o", required=True)
    arg_parser.add_argument("--testbed", "-m", required=True)
    args = arg_parser.parse_args()

    with open(args.input_json) as f:
        benchmarks_json = json.load(f)

    timestamp = datetime.datetime.fromisoformat(benchmarks_json["datetime"]).astimezone(
        pytz.UTC
    )
    commit_sha = benchmarks_json["commit_info"]["id"]
    commit_branch = benchmarks_json["commit_info"]["branch"]

    rows = []
    for benchmark in benchmarks_json["benchmarks"]:
        rows.append(
            {
                "testbed": args.testbed,
                "timestamp": timestamp.isoformat(),
                "commit_sha": commit_sha,
                "commit_branch": commit_branch,
                "benchmark_name": benchmark["name"],
                "benchmark_fullname": benchmark["fullname"],
                "benchmark_mean_s": benchmark["stats"]["mean"],
                "benchmark_stddev_s": benchmark["stats"]["stddev"],
                "benchmark_rounds": benchmark["stats"]["rounds"],
                "benchmark_iterations": benchmark["stats"]["iterations"],
            }
        )

    df = pl.DataFrame(data=rows, orient="row")

    output_path = (
        pathlib.Path(args.output_dir)
        / f"{commit_sha}_{timestamp.strftime('%Y%m%d%H%M%S')}.csv"
    )
    os.makedirs(output_path.parent, exist_ok=True)
    df.write_csv(output_path)


if __name__ == "__main__":
    main()
