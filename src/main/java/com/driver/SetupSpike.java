package com.driver;

import com.inmem.Transaction;
import com.pos.PosTxnReq;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static com.etl.FlinkKafkaConn.*;

public class SetupSpike {
    public static void main(String[] args) throws Exception {
        System.out.println("here");
        List<Transaction> txns = new ArrayList<Transaction>(){{
            add(new Transaction(1, 100, "AC1"));
            add(new Transaction(2, 10, "AC2"));
            add(new Transaction(3, 20, "AC3"));
        }};

        for (Transaction txn: txns) {
            System.out.println(txn.toJSONString());
        }

        List<PosTxnReq> postxns = new ArrayList<PosTxnReq>(){{
            add(new PosTxnReq(1, 101));
            add(new PosTxnReq(2, 123));
        }};

        for (PosTxnReq txn: postxns) {
            System.out.println(txn.toJSONString());
        }

        System.out.println("done");

        Tuple2<StreamExecutionEnvironment, Properties> tuple = setupConnection("groupId");
        sinkStream("transactions3", new DataStream<String>(tuple.f0, new Transformation<String>() {
            @Override
            public Collection<Transformation<?>> getTransitivePredecessors() {
                return null;
            }
        }));
    }
}
