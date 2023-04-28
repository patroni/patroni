#!/bin/bash

# Release process:
# 1. Open a PR that updates release notes and Patroni version
# 2. Merge it
# 3. Run release.sh
# 4. After the new tag is pushed, the .github/workflows/release.yaml will run tests and upload the new package to test.pypi.org
# 5. Once the release is created, the .github/workflows/release.yaml will run tests and upload the new package to pypi.org

## Bail out on any non-zero exitcode from the called processes
set -xe

if python3 --version &> /dev/null; then
    alias python=python3
    shopt -s expand_aliases
fi

python --version
git --version

version=$(python -c 'from patroni.version import __version__; print(__version__)')

python setup.py clean
python setup.py test
python setup.py flake8

git tag "v$version"
git push --tags
