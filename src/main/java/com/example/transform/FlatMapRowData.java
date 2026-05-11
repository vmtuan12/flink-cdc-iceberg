package com.example.transform;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class FlatMapRowData extends RichFlatMapFunction<RowData, RowData> {
    @Override
    public void flatMap(RowData rowData, Collector<RowData> collector) throws Exception {
        collector.collect(rowData);
    }
}
