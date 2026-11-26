"""``converge`` and ``status``, each printing one JSON line the server reads."""

from __future__ import annotations

import argparse
import json
import sys

from . import converge, units


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="toolset_runtime")
    commands = parser.add_subparsers(dest="command", required=True)
    converge_command = commands.add_parser("converge")
    converge_command.add_argument(
        "--strict", action="store_true", help="exit 1 when any unit failed or the table was refused"
    )
    status_command = commands.add_parser("status")
    for command in (converge_command, status_command):
        command.add_argument("--root", default=units.DEFAULT_ROOT)
        command.add_argument("--units", default=None, help="a table other than the shipped one")
    args = parser.parse_args(argv)
    try:
        table = units.load(args.units)
    except units.UnitsError as exc:
        print(f"toolset: {exc}", file=sys.stderr)
        return 2
    if args.command == "status":
        print(json.dumps(converge.status(args.root, table), sort_keys=True))
        return 0
    answer = converge.converge(args.root, table)
    print(answer.to_json())
    return 1 if args.strict and answer.failed else 0
