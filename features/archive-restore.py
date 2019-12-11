#!/usr/bin/env python
import os
import argparse
import shutil

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dirname", required=True)
    parser.add_argument("--pathname", required=True)
    parser.add_argument("--filename", required=True)
    parser.add_argument("--mode", required=True, choices=("archive", "restore"))
    args, _ = parser.parse_known_args()

    full_filename = os.path.join(args.dirname, args.filename)
    if args.mode == "archive":
        if not os.path.isdir(args.dirname):
            os.makedirs(args.dirname)
        if not os.path.exists(full_filename):
            shutil.copy(args.pathname, full_filename)
    else:
        shutil.copy(full_filename, args.pathname)
