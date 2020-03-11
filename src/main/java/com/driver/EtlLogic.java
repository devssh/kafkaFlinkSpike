package com.driver;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

import static com.etl.FlinkKafkaConn.*;
import static com.inmem.Transaction.mapTransactions;

public class EtlLogic {
    public static void main(String[] args) throws Exception {
        System.out.println("Begin");
        Tuple2<StreamExecutionEnvironment, Properties> tuple = setupConnection("groupId");
        System.out.println("Setup done");

        mapTxnETL(mapTransactions(), "transactions1", "txn1", tuple.f0, tuple.f1);
//        mapPosTxnETL(mapPosTransactions(), "postransactions1", "postxn1", tuple.f0, tuple.f1);
//        windowJoin("transactions2", "postransactions2", "expandedtxn1", tuple.f0, tuple.f1);
        System.out.println("Done");
    }
}
