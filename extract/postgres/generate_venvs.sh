#!/bin/bash
apt-get install python3-venv

mkdir -p venvs

python3 -m venv venvs/tap-postgres
venvs/tap-postgres/bin/pip3 install --upgrade pip
venvs/tap-postgres/bin/pip3 install --upgrade setuptools
venvs/tap-postgres/bin/pip3 install tap-postgres