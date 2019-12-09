#!/usr/bin/env python
import argparse
import subprocess
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--datadir", required=True)
    parser.add_argument("--dbname", required=True)
    parser.add_argument("--walmethod", required=True, choices=("fetch", "stream", "none"))
    args, _ = parser.parse_known_args()

    command = ["pg_basebackup", "-D", args.datadir] + \
              (["-X", args.walmethod] if args.walmethod != "none" else []) + \
              ["-c", "fast", "-d", args.dbname]
    sys.exit(subprocess.call(command))
