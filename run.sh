#!/bin/sh
set -eu
cd "$(dirname "$0")"
exec ./mvnw spring-boot:run '-Dspring-boot.run.jvmArguments=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED'
