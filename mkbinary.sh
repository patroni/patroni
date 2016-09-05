#!/bin/sh
set -e

pip install --ignore-installed -r requirements-bin.txt
pyinstaller --clean --onefile patroni.spec
