package com.etl;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

import static com.etl.StringUtils.stringToBytes;
import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;

public class FlinkKafkaConn {
    public static Tuple2<StreamExecutionEnvironment, Properties> setupConnection(String topic) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", topic);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return new Tuple2<StreamExecutionEnvironment, Properties>(env, properties);
    }

    public static DataStream<String> consumeStream(String topic, StreamExecutionEnvironment env, Properties properties) {
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        myConsumer.setStartFromEarliest();     // start from the earliest record possible
        DataStream<String> stream = env.addSource(myConsumer);
        return stream;
    }

    public static void sinkStream(String topic, DataStream<String> stream, MapFunction<String, String> etl, StreamExecutionEnvironment env, Properties properties) throws Exception {
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        try {
                            return new ProducerRecord<byte[], byte[]>(topic, stringToBytes(etl.map(element)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
                }, properties, Semantic.EXACTLY_ONCE);
        stream.addSink(myProducer);
        env.execute();
    }

    public static void printStream(MapFunction<String, String> etl, DataStream<String> stream) {
        DataStreamSink<String> sinkTxn = stream.map(etl).print();
    }

    public static void mapETL(MapFunction<String, String> etl, String inTopic, String outTopic, StreamExecutionEnvironment env, Properties properties) throws Exception {
        DataStream<String> txnStream = consumeStream(inTopic, env, properties);
        sinkStream(outTopic, txnStream, etl, env, properties);
    }
}
