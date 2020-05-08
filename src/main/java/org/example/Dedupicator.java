package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Dedupicator extends KeyedProcessFunction<String, Record, Record> {

    private static final long FIVE_SECONDS = 5 * 1000L;

    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) throws Exception {
        seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen-flag", Types.BOOLEAN));
    }

    @Override
    public void processElement(Record record, Context context, Collector<Record> collector) throws Exception {
        if (seen.value() != null) {
            // This is a duplicate
            return;
        }

        seen.update(true);
        long timer = context.timerService().currentProcessingTime() + FIVE_SECONDS;
        context.timerService().registerProcessingTimeTimer(timer);
        collector.collect(record);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Record> out) throws Exception {
        seen.clear();
    }
}
