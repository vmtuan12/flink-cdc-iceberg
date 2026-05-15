package com.example.sink;

import com.example.config.AppConfig;
import com.example.schema.DebeziumAvroToIcebergSchemaConverter;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds and attaches the Iceberg streaming sink backed by Hive Metastore + MinIO.
 *
 * Key design points:
 *  - Uses {@link FlinkSink#forRowData} which accepts Flink RowData (with RowKind).
 *  - Equality-delete based UPSERT is enabled when you supply equalityFieldColumns.
 *  - S3A / MinIO credentials are injected into the Hadoop Configuration so that
 *    the Iceberg writer can resolve s3a:// warehouse URIs against MinIO.
 *  - The table is auto-created if it does not already exist in the catalog.
 */
public final class IcebergSinkFactoryV2 {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFactoryV2.class);

    private IcebergSinkFactoryV2() {}

    /**
     * Creates the Iceberg table if necessary, then attaches a streaming sink
     * to {@code stream}.
     */
    public static void attachSink(DataStream<RowData> stream, AppConfig config) throws RestClientException, IOException {

        // ── 1. Hadoop config with MinIO / S3A settings ────────────────────────
        Configuration hadoopConf = buildHadoopConf(config);

        // ── 2. Iceberg / Hive catalog properties ─────────────────────────────
        Map<String, String> catalogProps = buildCatalogProperties(config);

        // ── 3. CatalogLoader (lazy – serialized to task managers) ─────────────
        CatalogLoader catalogLoader = CatalogLoader.hive(
                config.getCatalogName(),
                hadoopConf,
                catalogProps);

        // ── 4. Ensure table exists ────────────────────────────────────────────
        TableIdentifier tableId = TableIdentifier.of(
                config.getIcebergDatabase(), config.getIcebergTable());

        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(config.getSchemaRegistryUrl(), 1000);
        String schemaKeySubjectName = config.getKafkaTopic() + "-key";
        String schemaValueSubjectName = config.getKafkaTopic() + "-value";
        org.apache.avro.Schema keySchema = new org.apache.avro.Schema.Parser().parse(registryClient.getLatestSchemaMetadata(schemaKeySubjectName).getSchema());
        org.apache.avro.Schema valueSchema = new org.apache.avro.Schema.Parser().parse(registryClient.getLatestSchemaMetadata(schemaValueSubjectName).getSchema());

        List<org.apache.avro.Schema.Field> fields = keySchema.getFields();
        List<String> equalityFieldColumns = fields.stream().map(org.apache.avro.Schema.Field::name).toList();
        LOG.info("Equality field columns: {}", equalityFieldColumns.toString());

        ensureTableExists(catalogLoader, tableId, valueSchema);

        // ── 5. TableLoader ────────────────────────────────────────────────────
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        // ── 6. Attach FlinkSink ───────────────────────────────────────────────
        FlinkSink.forRowData(stream)
                .tableLoader(tableLoader)
                // Enable equality-delete upsert – list your primary key column(s) here.
                // Remove / adjust if your table is append-only.
                .equalityFieldColumns(equalityFieldColumns)
                // upsert mode: handles UPDATE_BEFORE / UPDATE_AFTER / DELETE correctly
                .upsert(true)
                .append();

        LOG.info("Iceberg sink attached → {}", config.getFullTableIdentifier());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Configuration buildHadoopConf(AppConfig cfg) {
        Configuration conf = new Configuration();

        // MinIO endpoint override
        conf.set("fs.s3a.endpoint",              cfg.getMinioEndpoint());
        conf.set("fs.s3a.access.key",            cfg.getMinioAccessKey());
        conf.set("fs.s3a.secret.key",            cfg.getMinioSecretKey());

        // Path-style access is required for MinIO (no virtual-hosted buckets)
        conf.set("fs.s3a.path.style.access",     "true");

        // Disable SSL for local/dev MinIO; set to true in production
        conf.set("fs.s3a.connection.ssl.enabled","false");

        // Use the AWS SDK v1 implementation bundled with hadoop-aws
        conf.set("fs.s3a.impl",
                 "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.AbstractFileSystem.s3a.impl",
                 "org.apache.hadoop.fs.s3a.S3A");

        // Performance tuning
        conf.set("fs.s3a.multipart.size",         "67108864");  // 64 MB
        conf.set("fs.s3a.fast.upload",            "true");
        conf.set("fs.s3a.fast.upload.buffer",     "disk");

        // Hive metastore address (also used by HiveCatalog internally)
        conf.set("hive.metastore.uris",           cfg.getHiveMetastoreUri());

        return conf;
    }

    private static Map<String, String> buildCatalogProperties(AppConfig cfg) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI,              cfg.getHiveMetastoreUri());
        props.put(CatalogProperties.WAREHOUSE_LOCATION, cfg.getIcebergWarehouse());

        // S3A credentials mirrored in catalog props for Iceberg's own FileIO
        props.put("s3.endpoint",            cfg.getMinioEndpoint());
        props.put("s3.access-key-id",       cfg.getMinioAccessKey());
        props.put("s3.secret-access-key",   cfg.getMinioSecretKey());
        props.put("s3.path-style-access",   "true");

        // Use HadoopFileIO so that s3a:// works via the Hadoop S3A connector
        props.put(CatalogProperties.FILE_IO_IMPL,
                  "org.apache.iceberg.hadoop.HadoopFileIO");

        return props;
    }

    /**
     * Auto-creates the Iceberg table when it does not exist yet.
     * In production you may prefer to create tables via DDL or Terraform.
     */
    private static void ensureTableExists(CatalogLoader catalogLoader,
                                          TableIdentifier tableId,
                                          org.apache.avro.Schema avroSchema) {
        try (HiveCatalog catalog = (HiveCatalog) catalogLoader.loadCatalog()) {
            if (!catalog.tableExists(tableId)) {
                Schema icebergSchema = DebeziumAvroToIcebergSchemaConverter.convert(avroSchema);

                // Simple unpartitioned table – add partitioning here if needed:
                // PartitionSpec.builderFor(schema).day("created_at").build()
                PartitionSpec spec = PartitionSpec.unpartitioned();

                Map<String, String> tableProps = new HashMap<>();
                tableProps.put("format-version", "2");  // required for equality-deletes
                tableProps.put("write.upsert.enabled", "true");

                catalog.createTable(tableId, icebergSchema, spec, tableProps);
                LOG.info("Created Iceberg table {}", tableId);
            } else {
                LOG.info("Iceberg table {} already exists", tableId);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure Iceberg table exists: " + tableId, e);
        }
    }
}
