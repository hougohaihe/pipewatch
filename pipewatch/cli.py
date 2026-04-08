"""Minimal CLI entry-point for pipewatch.

Usage examples:
    pipewatch run --name my_pipeline --module mypackage.pipelines --fn run_etl
    pipewatch run --name my_pipeline --module mypackage.pipelines --fn run_etl \
        --log-dir ./logs --alert-config alerts.json
"""

import argparse
import importlib
import sys

from pipewatch.run_logger import RunLogger
from pipewatch.alert_config import load_dispatcher_from_file
from pipewatch.pipeline_runner import PipelineRunner


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and log data pipeline runs.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_cmd = sub.add_parser("run", help="Execute a pipeline function.")
    run_cmd.add_argument("--name", required=True, help="Human-readable pipeline name.")
    run_cmd.add_argument("--module", required=True, help="Dotted module path containing the function.")
    run_cmd.add_argument("--fn", required=True, help="Function name to call.")
    run_cmd.add_argument("--log-dir", default="./pipewatch_logs", help="Directory for run logs.")
    run_cmd.add_argument("--alert-config", default=None, help="Path to JSON alert config file.")

    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)

    if args.command == "run":
        # Resolve the target function
        try:
            module = importlib.import_module(args.module)
        except ModuleNotFoundError as exc:
            print(f"[pipewatch] ERROR: Cannot import module '{args.module}': {exc}", file=sys.stderr)
            sys.exit(1)

        fn = getattr(module, args.fn, None)
        if fn is None or not callable(fn):
            print(f"[pipewatch] ERROR: '{args.fn}' not found or not callable in '{args.module}'.", file=sys.stderr)
            sys.exit(1)

        logger = RunLogger(log_dir=args.log_dir)

        dispatcher = None
        if args.alert_config:
            try:
                dispatcher = load_dispatcher_from_file(args.alert_config)
            except Exception as exc:  # noqa: BLE001
                print(f"[pipewatch] WARNING: Could not load alert config: {exc}", file=sys.stderr)

        runner = PipelineRunner(name=args.name, logger=logger, dispatcher=dispatcher)
        runner.run(fn)


if __name__ == "__main__":  # pragma: no cover
    main()
