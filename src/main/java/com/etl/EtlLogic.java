package com.etl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

import static com.etl.FlinkKafkaConn.mapETL;
import static com.etl.FlinkKafkaConn.setupConnection;
import static com.inmem.Transaction.mapTransactions;
import static com.pos.PosTxnReq.mapPosTransactions;

public class EtlLogic {
    public static void main(String[] args) throws Exception {
        System.out.println("Begin");
        Tuple2<StreamExecutionEnvironment, Properties> tuple = setupConnection("groupId");
        System.out.println("Setup done");

        mapETL(mapTransactions(), "transactions1", "txn1", tuple.f0, tuple.f1);
        mapETL(mapPosTransactions(), "postransactions1", "postxn1", tuple.f0, tuple.f1);
        System.out.println("Done");
    }
}
