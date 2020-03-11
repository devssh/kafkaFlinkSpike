package com.etl;

import com.inmem.Transaction;
import com.pos.PosTxnReq;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class EnrichmentFunction extends RichCoFlatMapFunction<Transaction, PosTxnReq, PosTxnExpanded> {
    public ValueState<PosTxnReq> reqState;
    public List<PosTxnReq> postxns = new ArrayList<PosTxnReq>(){{
        add(new PosTxnReq(1, 101));
        add(new PosTxnReq(2, 123));
        add(new PosTxnReq(3, 131));
    }};

    @Override
    public void open(Configuration config) {

    }

    @Override
    public void flatMap1(Transaction txn, Collector<PosTxnExpanded> out) throws Exception {
        out.collect(new PosTxnExpanded(txn, postxns.stream().filter(req->
                req.txnID==txn.txnID).collect(toList()).get(0)));
    }

    @Override
    public void flatMap2(PosTxnReq postxn, Collector<PosTxnExpanded> out) throws Exception {
        out.collect(new PosTxnExpanded(new Transaction(10, 10.0f, "ACCX"), postxn));
    }
}
