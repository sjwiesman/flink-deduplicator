package org.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RecordDeserializationSchema implements KafkaDeserializationSchema<Record> {
    @Override
    public boolean isEndOfStream(Record record) {
        return false;
    }

    @Override
    public Record deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        Record record = new Record();
        record.setKey(new String(consumerRecord.key()));
        record.setMessage(new String(consumerRecord.value()));
        return record;
    }

    @Override
    public TypeInformation<Record> getProducedType() {
        return TypeInformation.of(Record.class);
    }
}
