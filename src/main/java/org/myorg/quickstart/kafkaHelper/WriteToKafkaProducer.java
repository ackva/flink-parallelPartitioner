package org.myorg.quickstart.kafkaHelper;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.KafkaSink;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.myorg.quickstart.partitioners.GraphPartitionerKafkaReservoir;

import java.util.ArrayList;
import java.util.Properties;

public class WriteToKafkaProducer {

    private static String inputPath = null;
    public static int k = 2; // parallelism - partitions
    public static String topic = "test";


    public static void main (String[] args) throws Exception {

        parseParameters(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Connector (experimental!!)
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //ParameterTool parameterTool = ParameterTool.fromArgs(args);

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(topic, new SimpleStringSchema(), properties);
        //myConsumer.setStartFromEarliest();

        // Get input from Kafka (must be up with topic "graphRead1")
        DataStreamSource streamInput = env.readTextFile(inputPath);
        streamInput.print();

        streamInput.addSink(new FlinkKafkaProducer011(topic, new SimpleStringSchema(), properties));

        JobExecutionResult result = env.execute("Writing to Kafka for next Job");

        System.out.println(" ############ ");

    }

    private static void parseParameters(String[] args) throws Exception {

        if (args.length > 0) {
            inputPath = args[0];
            k = Integer.valueOf(args[1]);
            topic = args[2];
        } else {
            System.out.println("Please provide parameters.");
            System.out.println(" --> Usage: PhasePartitioner <TODO>");
        }
    }

}
