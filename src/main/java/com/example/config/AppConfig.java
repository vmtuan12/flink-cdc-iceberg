package com.example.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.Properties;

/**
 * Centralises every tunable parameter.
 * Pass values as --key value on the Flink job CLI or as Java system properties.
 */
public class AppConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── Kafka ─────────────────────────────────────────────────────────────────
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String kafkaGroupId;

    private final String schemaRegistryUrl;

    // ── Iceberg / Hive catalog ────────────────────────────────────────────────
    private final String catalogName;
    private final String hiveMetastoreUri;
    private final String icebergWarehouse;   // s3a://bucket/warehouse
    private final String icebergDatabase;
    private final String icebergTable;

    // ── MinIO / S3A ───────────────────────────────────────────────────────────
    private final String minioEndpoint;
    private final String minioAccessKey;
    private final String minioSecretKey;


    // ── Runtime ───────────────────────────────────────────────────────────────
    private final String jobName;
    private final long checkpointIntervalMs;
    private final int parallelism;

    // ─────────────────────────────────────────────────────────────────────────

    public AppConfig() {
        this.kafkaBootstrapServers = "localhost:9092";
        this.kafkaTopic            = "dbserver1.public.orders";
        this.kafkaGroupId          = "test-111";
        this.schemaRegistryUrl = "http://localhost:8081";

        this.catalogName           = "teko_datawarehouse";
        this.hiveMetastoreUri      = "thrift://localhost:9083";
        this.icebergWarehouse      = "s3a://iceberg/";
        this.icebergDatabase       = "default";
        this.icebergTable          = "orders";

        this.minioEndpoint         = "http://localhost:9000";
        this.minioAccessKey        = "root";
        this.minioSecretKey        = "root123456";

        this.jobName               = "debezium-cdc-iceberg";
        this.checkpointIntervalMs  = 30_000L;
        this.parallelism  = 2;
    }

    private AppConfig(ParameterTool params) {
        this.kafkaBootstrapServers = params.get("kafka.bootstrap.servers", "kafka:29092");
        this.kafkaTopic            = params.get("kafka.topic", "dbserver1.public.orders");
        this.kafkaGroupId          = params.get("kafka.group.id", "flink-cdc-iceberg");
        this.schemaRegistryUrl     = params.get("schema.registry.url", "http://schema-registry:8081");

        this.catalogName           = params.get("iceberg.catalog.name", "teko_datawarehouse");
        this.hiveMetastoreUri      = params.get("hive.metastore.uri", "thrift://hive-metastore:9083");
        this.icebergWarehouse      = params.get("iceberg.warehouse", "s3a://iceberg/");
        this.icebergDatabase       = params.get("iceberg.database", "default");
        this.icebergTable          = params.get("iceberg.table", "orders");

        this.minioEndpoint         = params.get("minio.endpoint", "http://minio:9000");
        this.minioAccessKey        = params.get("minio.access.key", "root");
        this.minioSecretKey        = params.get("minio.secret.key", "root123456");

        this.jobName               = params.get("job.name", "debezium-cdc-iceberg");
        this.checkpointIntervalMs  = params.getLong("checkpoint.interval.ms", 30_000L);
        this.parallelism  = params.getInt("parallelism", 2);
    }

    public static AppConfig fromArgs(String[] args) {
        return new AppConfig(ParameterTool.fromArgs(args));
    }

    // ── Kafka consumer Properties ─────────────────────────────────────────────
    public Properties kafkaConsumerProperties() {
        Properties props = new Properties();
        props.setProperty("security.protocol", "PLAINTEXT");
        // Increase fetch sizes for large CDC payloads
        props.setProperty("max.partition.fetch.bytes", "10485760");
        props.setProperty("fetch.max.bytes", "52428800");
        return props;
    }

    // ── Getters ───────────────────────────────────────────────────────────────
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getKafkaTopic()            { return kafkaTopic; }
    public String getKafkaGroupId()          { return kafkaGroupId; }
    public String getSchemaRegistryUrl()     { return schemaRegistryUrl; }
    public String getCatalogName()           { return catalogName; }
    public String getHiveMetastoreUri()      { return hiveMetastoreUri; }
    public String getIcebergWarehouse()      { return icebergWarehouse; }
    public String getIcebergDatabase()       { return icebergDatabase; }
    public String getIcebergTable()          { return icebergTable; }
    public String getMinioEndpoint()         { return minioEndpoint; }
    public String getMinioAccessKey()        { return minioAccessKey; }
    public String getMinioSecretKey()        { return minioSecretKey; }
    public String getJobName()               { return jobName; }
    public long   getCheckpointIntervalMs()  { return checkpointIntervalMs; }
    public int getParallelism() { return parallelism; }

    public String getFullTableIdentifier() {
        return icebergDatabase + "." + icebergTable;
    }

    @Override
    public String toString() {
        return "AppConfig{" +
               "kafka=" + kafkaBootstrapServers +
               ", topic=" + kafkaTopic +
               ", hms=" + hiveMetastoreUri +
               ", warehouse=" + icebergWarehouse +
               ", table=" + getFullTableIdentifier() +
               '}';
    }
}
