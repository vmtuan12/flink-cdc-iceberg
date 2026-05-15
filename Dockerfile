FROM flink:1.19.1-scala_2.12-java17

# Copy extra lib jars onto the Flink classpath
COPY flink/lib/*.jar /opt/flink/lib/

# Copy plugins (each plugin must be in its own subdirectory)
COPY flink/plugins/ /opt/flink/plugins/

COPY target/flink-cdc-iceberg-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-cdc-iceberg-1.0-SNAPSHOT.jar
