#!/usr/bin/env python
import argparse
import shutil

from time import sleep

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--datadir", required=True)
    parser.add_argument("--sourcedir", required=True)
    parser.add_argument("--test-argument", required=True)
    parser.add_argument("--sleep", required=False, type=int)
    args, _ = parser.parse_known_args()

    if args.sleep:
        sleep(args.sleep)

    shutil.copytree(args.sourcedir, args.datadir)
