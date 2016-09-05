#!/bin/sh
set -e

pip install --ignore-installed setuptools==19.2 pyinstaller
pyinstaller --clean --onefile patroni.spec
