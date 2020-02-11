package com.etl;

import com.inmem.Transaction;
import com.pos.PosTxnReq;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

import static com.etl.StringUtils.stringToBytes;
import static com.etl.StringUtils.timestampNow;
import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;

public class FlinkKafkaConn {
    public static Tuple2<StreamExecutionEnvironment, Properties> setupConnection(String topic) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", topic);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return new Tuple2<StreamExecutionEnvironment, Properties>(env, properties);
    }

    public static DataStream<String> consumeStream(String topic, StreamExecutionEnvironment env, Properties properties) {
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        myConsumer.setStartFromEarliest();     // start from the earliest record possible
        DataStream<String> stream = env.addSource(myConsumer);
        return stream;
    }

    public static void sinkStream(String topic, DataStream<String> stream, MapFunction<String, String> etl, StreamExecutionEnvironment env, Properties properties) throws Exception {
        System.out.println("here3");
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        try {
                            System.out.println(element);
                            return new ProducerRecord<byte[], byte[]>(topic, stringToBytes(etl.map(element)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
                }, properties, Semantic.EXACTLY_ONCE);
//        stream.timeWindowAll(Time.minutes(1));
        stream.addSink(myProducer);
        env.execute();
    }

    public static void sinkStream(String topic, DataStream<String> stream, StreamExecutionEnvironment env, Properties properties) throws Exception {
        sinkStream(topic, stream, new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }, env, properties);
    }

    public static void printStream(MapFunction<String, String> etl, DataStream<String> stream) {
        DataStreamSink<String> sinkTxn = stream.map(etl).print();
    }

    public static void mapTxnETL(MapFunction<String, String> etl, String inTopic, String outTopic, StreamExecutionEnvironment env, Properties properties) throws Exception {
        DataStream<Transaction> txnStream = consumeStream(inTopic, env, properties).map(Transaction::fromJSONString).keyBy("txnID");
        sinkStream(outTopic, txnStream.map(Transaction::toJSONString), etl, env, properties);
    }

    public static void mapPosTxnETL(MapFunction<String, String> etl, String inTopic, String outTopic, StreamExecutionEnvironment env, Properties properties) throws Exception {
        DataStream<PosTxnReq> postxnStream = consumeStream(inTopic, env, properties).map(PosTxnReq::fromJSONString).keyBy("txnID");
//                .window();
        sinkStream(outTopic, postxnStream.map(PosTxnReq::toJSONString), etl, env, properties);
    }

    public static void windowJoin(String inTopic1, String inTopic2, String outTopic, StreamExecutionEnvironment env, Properties properties) throws Exception {
        DataStream<Transaction> txnStream = consumeStream(inTopic1, env, properties).map(Transaction::fromJSONString).keyBy(x->x.txnID).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Transaction>() {
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(timestampNow());
            }

            @Override
            public long extractTimestamp(Transaction element, long previousElementTimestamp) {
                return timestampNow();
            }
        });
        DataStream<PosTxnReq> postxnStream = consumeStream(inTopic2, env, properties).map(PosTxnReq::fromJSONString).keyBy(x->x.txnID).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<PosTxnReq>() {
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(timestampNow());
            }

            @Override
            public long extractTimestamp(PosTxnReq element, long previousElementTimestamp) {
                return timestampNow();
            }
        });

        DataStream<PosTxnExpanded> outputStream = postxnStream.join(txnStream).where(new KeySelector<PosTxnReq, Integer>() {
            public Integer getKey(PosTxnReq req) {
                return req.txnID;
            }
        }).equalTo(new KeySelector<Transaction, Integer>() {
            @Override
            public Integer getKey(Transaction txn) throws Exception {
                return txn.txnID;
            }
        }).window(TumblingEventTimeWindows.of(Time.days(1))).apply(new JoinFunction<PosTxnReq, Transaction, PosTxnExpanded>() {
            @Override
            public PosTxnExpanded join(PosTxnReq req, Transaction txn) throws Exception {
                return new PosTxnExpanded(txn, req);
            }
        });
        outputStream.print();
        System.out.println("printing output stream done");
        sinkStream(outTopic, outputStream.map(PosTxnExpanded::toJSONString), env, properties);
    }
}
