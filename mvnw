#!/bin/sh
set -eu
cd "$(dirname "$0")"
if [ -z "${JAVA_HOME:-}" ]; then
 export JAVA_HOME="$PWD/.tools/amazon-corretto-17.jdk/Contents/Home"
fi
MAVEN_BIN='/Applications/IntelliJ IDEA.app/Contents/plugins/maven/lib/maven3/bin/mvn'
if command -v mvn >/dev/null 2>&1; then MAVEN_BIN=$(command -v mvn); fi
exec "$MAVEN_BIN" -Dmaven.repo.local="$PWD/.tools/repository" "$@"
