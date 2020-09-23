#!/usr/bin/env sh

set -e

java -Dconfig.file=$APP_CONFIG_FILE -DHOST_NAME="$HOST_NAME" -jar app.jar