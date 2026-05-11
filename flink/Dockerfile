FROM flink:1.19.1-scala_2.12-java17

COPY conf/hive-site.xml /opt/flink/conf/hive-site.xml

# Copy extra lib jars onto the Flink classpath
COPY lib/*.jar /opt/flink/lib/

# Copy plugins (each plugin must be in its own subdirectory)
COPY plugins/ /opt/flink/plugins/

COPY docker-entrypoint-init.sh /opt/flink/docker-entrypoint-init.sh
RUN chmod +x /opt/flink/docker-entrypoint-init.sh

ENTRYPOINT ["/opt/flink/docker-entrypoint-init.sh"]
