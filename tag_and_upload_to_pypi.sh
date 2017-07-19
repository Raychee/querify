#!/usr/bin/env bash


# Before running this script, remember to git commit and push!


VERSION=$(grep ^VERSION setup.py | sed "s/^VERSION = '//" | sed "s/'$//")
COMMENT=$(git log -1 --pretty=%B | cat)

git tag ${VERSION} -m "${COMMENT}" && \
git push origin --tags && \
python setup.py sdist upload -r pypi
