#!/bin/sh
set -e

pip install --ignore-installed pyinstaller
pyinstaller --clean patroni.spec
