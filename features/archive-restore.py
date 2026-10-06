#!/usr/bin/env python
import argparse
import os
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
            # Copy to a temporary name, then rename. A restore on another node
            # must never see a partly written segment.
            tmp_filename = full_filename + ".tmp." + str(os.getpid())
            shutil.copy(args.pathname, tmp_filename)
            os.replace(tmp_filename, full_filename)
    else:
        shutil.copy(full_filename, args.pathname)
