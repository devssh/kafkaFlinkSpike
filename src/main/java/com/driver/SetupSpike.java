package com.driver;

import com.etl.PosTxnExpanded;
import com.inmem.Transaction;
import com.pos.PosTxnReq;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.etl.FlinkKafkaConn.*;
import static com.etl.PosTxnExpanded.idealScenario;

public class SetupSpike {
    public static void main(String[] args) throws Exception {
        System.out.println("here");
        List<Transaction> txns1 = new ArrayList<Transaction>() {{
            add(new Transaction(1, 100, "AC1"));
            add(new Transaction(2, 10, "AC2"));
            add(new Transaction(3, 20, "AC3"));
        }};

        Tuple2<StreamExecutionEnvironment, Properties> conn1 = setupConnection("transaction1");
        StreamExecutionEnvironment env = conn1.f0;
        Properties properties = conn1.f1;

        DataStream<String> txns = env.fromCollection(txns1).map(Transaction::toJSONString);
        sinkStream("transactions1", txns, env, properties);
        System.out.println("sunk successfully");
        for (Transaction txn : txns1) {
            System.out.println(txn.toJSONString());
        }

        List<PosTxnReq> postxns1 = new ArrayList<PosTxnReq>() {{
            add(new PosTxnReq(1, 101));
            add(new PosTxnReq(2, 123));
//            add(new PosTxnReq(3, 131));
        }};
//        DataStream<PosTxnReq> postxns = env.fromCollection(postxns1).keyBy(x->x.txnID);
        DataStream<Transaction> txnStream = consumeStream("transactions1",
                env, properties).map(Transaction::fromJSONString).keyBy(x -> x.txnID);
        DataStream<PosTxnReq> postxns = consumeStream("transactions2",
                env, properties).map(PosTxnReq::fromJSONString).keyBy(x -> x.txnID);
//        DataStream<Transaction> txnStream = env.fromCollection(txns1).keyBy(x->x.txnID);
        System.out.println("streams");
        DataStream<PosTxnExpanded> expandedDataStream = txnStream.join(postxns).where(new KeySelector<Transaction, Integer>() {
            @Override
            public Integer getKey(Transaction value) throws Exception {
                return value.txnID;
            }
        }).equalTo(new KeySelector<PosTxnReq, Integer>() {
            @Override
            public Integer getKey(PosTxnReq value) throws Exception {
                return value.txnID;
            }
//        }).window(GlobalWindows.create())
//        }).window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
        }).window(GlobalWindows.create()).allowedLateness(Time.seconds(30)).apply(new JoinFunction<Transaction, PosTxnReq, PosTxnExpanded>() {
            @Override
            public PosTxnExpanded join(Transaction txn, PosTxnReq req) throws Exception {
                return new PosTxnExpanded(txn, req);
            }
        });
//        DataStream<PosTxnExpanded> posTxnExpanded = txnStream.connect(postxns).flatMap(new EnrichmentFunction());
//        sinkStream("txn2", posTxnExpanded.map(PosTxnExpanded::toJSONString), env, properties);

        System.out.println("output successful");
        for (PosTxnReq txn : postxns1) {
            System.out.println(txn.toJSONString());
        }

        System.out.println("created expanded stream");
        idealScenario();
        expandedDataStream.print();
        sinkStream("txn2", expandedDataStream.map(PosTxnExpanded::toJSONString), env, properties);

        System.out.println("done");
    }
}
