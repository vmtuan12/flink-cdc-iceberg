#!/bin/bash
# =============================================================================
# download-jars.sh
# Downloads all extra jars required by the Flink CDC → Iceberg pipeline.
#
# Usage:
#   chmod +x download-jars.sh
#   ./download-jars.sh
# =============================================================================
set -euo pipefail

MAVEN_CENTRAL="https://repo1.maven.org/maven2"

LIB_DIR="$(dirname "$0")/flink/lib"
PLUGIN_DIR="$(dirname "$0")/flink/plugins/s3-fs-hadoop"

mkdir -p "$LIB_DIR"
mkdir -p "$PLUGIN_DIR"

# ── Helper ────────────────────────────────────────────────────────────────────
download() {
  local url="$1"
  local dest="$2"
  local filename
  filename="$(basename "$dest")"

  if [ -f "$dest" ]; then
    echo "[SKIP] $filename already exists"
    return
  fi

  echo "[DOWN] $filename"
  if ! curl -fsSL --retry 3 --retry-delay 2 "$url" -o "$dest"; then
    echo "[FAIL] Could not download $url" >&2
    rm -f "$dest"
    exit 1
  fi
  echo "[ OK ] $filename"
}

# =============================================================================
# PLUGINS — s3-fs-hadoop
# Flink's bundled S3/MinIO filesystem plugin (includes hadoop-aws + AWS SDK).
# Must live in its own subdirectory under /opt/flink/plugins/.
# =============================================================================
echo ""
echo "=== Flink S3 plugin ==="

download \
  "$MAVEN_CENTRAL/org/apache/flink/flink-s3-fs-hadoop/1.19.1/flink-s3-fs-hadoop-1.19.1.jar" \
  "$PLUGIN_DIR/flink-s3-fs-hadoop-1.19.1.jar"

# =============================================================================
# LIB — Iceberg
# iceberg-flink-runtime is a pre-shaded fat jar that includes iceberg-core,
# iceberg-api, iceberg-hive-metastore, bundled-guava, etc.
# Do NOT add other iceberg-* jars alongside it.
# =============================================================================
echo ""
echo "=== Iceberg ==="

download \
  "$MAVEN_CENTRAL/org/apache/iceberg/iceberg-flink-runtime-1.19/1.8.0/iceberg-flink-runtime-1.19-1.8.0.jar" \
  "$LIB_DIR/iceberg-flink-runtime-1.19-1.8.0.jar"

# =============================================================================
# LIB — Hive Metastore client
# =============================================================================
# =============================================================================
# LIB — Hadoop client
# The Flink base image includes hadoop-common but NOT hadoop-mapreduce-client-core.
# =============================================================================
echo ""
echo "=== Hadoop ==="
download \
  "$MAVEN_CENTRAL/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar" \
  "$LIB_DIR/hadoop-common-3.3.4.jar"

download \
  "$MAVEN_CENTRAL/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar" \
  "$LIB_DIR/hadoop-auth-3.3.4.jar"

download \
  "$MAVEN_CENTRAL/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar" \
  "$LIB_DIR/hadoop-hdfs-client-3.3.4.jar"

download \
  "$MAVEN_CENTRAL/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar" \
  "$LIB_DIR/hadoop-mapreduce-client-core-3.3.4.jar"

download \
  "$MAVEN_CENTRAL/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar" \
  "$LIB_DIR/bundle-2.20.18.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
  "$LIB_DIR/hadoop-aws-3.3.4.jar"

download \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar" \
  "$LIB_DIR/aws-java-sdk-bundle-1.12.648.jar"
  
download \
  "$MAVEN_CENTRAL/software/amazon/awssdk/s3/2.20.18/s3-2.20.18.jar" \
  "$LIB_DIR/s3-2.20.18.jar"
  
download \
  "$MAVEN_CENTRAL/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar" \
  "$LIB_DIR/flink-connector-kafka-3.2.0-1.19.jar"
  
download \
  "$MAVEN_CENTRAL/org/apache/kafka/kafka-clients/3.7.1/kafka-clients-3.7.1.jar" \
  "$LIB_DIR/kafka-clients-3.7.1.jar"
  
download \
  "$MAVEN_CENTRAL/org/apache/flink/flink-parquet/1.19.1/flink-parquet-1.19.1.jar" \
  "$LIB_DIR/flink-parquet-1.19.1.jar"
#
#download \
#  "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" \
#  "$LIB_DIR/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"

download \
  "$MAVEN_CENTRAL/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.19.1/flink-sql-connector-hive-3.1.3_2.12-1.19.1.jar" \
  "$LIB_DIR/flink-sql-connector-hive-3.1.3_2.12-1.19.1.jar"

download \
  "$MAVEN_CENTRAL/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar" \
  "$LIB_DIR/hadoop-shaded-guava-1.1.1.jar"

download \
  "$MAVEN_CENTRAL/org/apache/hadoop/thirdparty/hadoop-shaded-protobuf_3_7/1.1.1/hadoop-shaded-protobuf_3_7-1.1.1.jar" \
  "$LIB_DIR/hadoop-shaded-protobuf_3_7-1.1.1.jar"


download \
  "$MAVEN_CENTRAL/org/apache/flink/flink-sql-parquet/1.19.1/flink-sql-parquet-1.19.1.jar" \
  "$LIB_DIR/flink-sql-parquet-1.19.1.jar"

download \
  "https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar" \
  "$LIB_DIR/commons-logging-1.1.3.jar"

download \
  "$MAVEN_CENTRAL/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar" \
  "$LIB_DIR/stax2-api-4.2.1.jar"

download \
  "https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar" \
  "$LIB_DIR/woodstox-core-5.3.0.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar" \
  "$LIB_DIR/commons-configuration2-2.1.1.jar"

download \
  "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.6.2/kafka-schema-registry-client-7.6.2.jar" \
  "$LIB_DIR/kafka-schema-registry-client-7.6.2.jar"

download \
  "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.6.2/kafka-avro-serializer-7.6.2.jar" \
  "$LIB_DIR/kafka-avro-serializer-7.6.2.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar" \
  "$LIB_DIR/avro-1.11.3.jar"

download \
  "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar" \
  "$LIB_DIR/jackson-databind-2.15.2.jar"

download \
  "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar" \
  "$LIB_DIR/jackson-core-2.15.2.jar"

download \
  "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar" \
  "$LIB_DIR/jackson-annotations-2.15.2.jar"

download \
  "https://repo1.maven.org/maven2/com/google/guava/guava/32.1.3-jre/guava-32.1.3-jre.jar" \
  "$LIB_DIR/guava-32.1.3-jre.jar"

download \
  "https://repo1.maven.org/maven2/io/debezium/debezium-core/2.7.0.Final/debezium-core-2.7.0.Final.jar" \
  "$LIB_DIR/debezium-core-2.7.0.Final.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/kafka/connect-api/3.5.1/connect-api-3.5.1.jar" \
  "$LIB_DIR/kafka-connect-api-3.5.1.jar"


# =============================================================================
# Summary
# =============================================================================
echo ""
echo "============================================"
echo "All jars downloaded successfully."
echo ""
echo "lib/     → $(ls "$LIB_DIR" | wc -l) jars"
echo "plugins/ → $(ls "$PLUGIN_DIR" | wc -l) jars"
echo ""
echo "Next steps:"
echo "  docker compose -f docker-compose.flink.yml build"
echo "  docker compose -f docker-compose.flink.yml up -d"
echo "============================================"
