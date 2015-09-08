#!/bin/sh

if [ $# -ne 1 ]; then
    >&2 echo "usage: $0 <version>"
    exit 1
fi

readonly VERSIONFILE="patroni/version.py"

## Bail out on any non-zero exitcode from the called processes
set -xe

python3 --version
git --version

version=$1

sed -i "s/__version__ = .*/__version__ = '${version}'/"  "${VERSIONFILE}"
python3 setup.py clean
python3 setup.py test
python3 setup.py flake8

git add "${VERSIONFILE}"

git commit -m "Bumped version to $version"
git push

python3 setup.py sdist bdist_wheel upload

git tag v${version}
git push --tags
