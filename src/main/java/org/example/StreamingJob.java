package org.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        List<String> topics = Arrays.asList(params.get("topics").split(","));

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, params.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();

		env
                .addSource(new FlinkKafkaConsumer<>(topics, new RecordDeserializationSchema(), properties))
                .keyBy(Record::getKey)
                .process(new Dedupicator())
                .print();

		env.execute("Flink Deduplication Job");
	}
}
