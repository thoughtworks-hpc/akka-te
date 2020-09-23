#!/usr/bin/env sh

set -e

java -Dconfig.file=./docker/$APP_CONFIG_FILE -DHOST_NAME="$HOST_NAME" -jar target/app-1.0-allinone.jar