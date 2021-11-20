#!/bin/bash
set -euo pipefail
git submodule foreach git checkout master
git submodule foreach git reset --hard origin/master
git submodule foreach git clean -fxd
git submodule foreach git pull
