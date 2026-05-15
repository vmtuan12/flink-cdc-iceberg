package com.example;

import com.example.config.AppConfig;
import com.example.deserialization.DebeziumAvroToRowDataDeserializer;
import com.example.sink.IcebergSinkFactoryV2;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;


public class CdcToIcebergJob {

    private static final Logger LOG = LoggerFactory.getLogger(CdcToIcebergJob.class);

    public static void main(String[] args) throws Exception {
        AppConfig appConfig = AppConfig.fromArgs(args);
        LOG.info("Starting CDC-to-Iceberg job with config: {}", appConfig);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(appConfig.getCheckpointIntervalMs());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(appConfig.getCheckpointIntervalMs());
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Configuration flinkConfig = new Configuration();

        flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3a://iceberg/flink-checkpoints/" + appConfig.getJobName());
        flinkConfig.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "s3a://iceberg/flink-savepoints/" + appConfig.getJobName());

        env.configure(flinkConfig);
        env.setParallelism(appConfig.getParallelism());

        KafkaSource<RowData> kafkaSource = KafkaSource.<RowData>builder()
                .setBootstrapServers(appConfig.getKafkaBootstrapServers())
                .setTopics(appConfig.getKafkaTopic())
                .setGroupId(appConfig.getKafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setDeserializer(new DebeziumAvroToRowDataDeserializer(appConfig.getSchemaRegistryUrl()))
                .setProperties(appConfig.kafkaConsumerProperties())
                .build();

        DataStream<RowData> cdcStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-CDC-Source")
                .name("kafka-cdc-source")
                .uid("kafka-cdc-source");

//        cdcStream.print();

        DataStream<RowData> rowDataStream = cdcStream
                .flatMap(new com.example.transform.FlatMapRowData())
                .name("cdc-to-rowdata")
                .uid("cdc-to-rowdata");

        IcebergSinkFactoryV2.attachSink(rowDataStream, appConfig);

        env.execute(appConfig.getJobName());
    }
}
