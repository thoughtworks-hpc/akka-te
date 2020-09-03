#!/usr/bin/env bash

set -e

mvn package

docker build -t akka-te .