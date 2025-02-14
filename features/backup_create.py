#!/usr/bin/env python
import argparse
import subprocess
import sys

from time import sleep

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--datadir", required=True)
    parser.add_argument("--dbname", required=True)
    parser.add_argument("--walmethod", required=True, choices=("fetch", "stream", "none"))
    parser.add_argument("--sleep", required=False, type=int)
    args, _ = parser.parse_known_args()

    if args.sleep:
        sleep(args.sleep)

    walmethod = ["-X", args.walmethod] if args.walmethod != "none" else []
    sys.exit(subprocess.call(["pg_basebackup", "-D", args.datadir, "-c", "fast", "-d", args.dbname] + walmethod))
