#!/bin/bash
set -euo pipefail

# Resolve hive-metastore container IP and register a clean hostname
# to avoid URISyntaxException from underscore-suffixed Docker DNS names
HMS_IP=$(getent hosts hive-metastore | awk '{ print $1 }')

if [ -n "$HMS_IP" ]; then
    echo "$HMS_IP hms-server.local" >> /etc/hosts
    echo "Registered hms-server.local -> $HMS_IP"
else
    echo "WARNING: Could not resolve hive-metastore hostname" >&2
fi

# Hand off to the original Flink entrypoint
exec /docker-entrypoint.sh "$@"
