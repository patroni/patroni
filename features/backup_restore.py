#!/usr/bin/env python
import argparse
import shutil

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--datadir", required=True)
    parser.add_argument("--sourcedir", required=True)
    args, _ = parser.parse_known_args()

    shutil.copytree(args.sourcedir, args.datadir)
