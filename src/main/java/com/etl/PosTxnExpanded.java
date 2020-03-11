package com.etl;

import com.google.gson.Gson;
import com.inmem.Transaction;
import com.pos.PosTxnReq;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class PosTxnExpanded {
    public static final int missingMCCValue = 100;
    final int txnID;
    final float amt;
    final String account;
    final int MCC;

    public PosTxnExpanded(Transaction inmem, PosTxnReq req) {
        if (inmem.txnID != req.txnID) {
            throw new NullPointerException("txnID do not match in PosTXN and TXN");
        }
        txnID = inmem.txnID;
        amt = inmem.amt;
        account = inmem.account;
        MCC = req.MCC;
    }

    public static PosTxnExpanded emptyPosTxnExpanded(Transaction inmem) {
        return new PosTxnExpanded(inmem, new PosTxnReq(inmem.txnID, missingMCCValue));
    }

    public String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public static PosTxnExpanded fromJSONString(String some) {
        Gson gson = new Gson();
        return gson.fromJson(some, PosTxnExpanded.class);
    }

    public static MapFunction<String, String> mapExpandedTransactions(List<PosTxnReq> postxns) {
        System.out.println("reached here");
        MapFunction<String, String> map = new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value != null || value.trim().length() > 0) {
                    try {
                        System.out.println("printing expanded input");
                        System.out.println(value);
                        Transaction txn = Transaction.fromJSONString(value);
                        PosTxnReq req = postxns.stream().filter(postxn ->
                                postxn.txnID == txn.txnID).collect(toList()).get(0);
                        return txn.expand(req).toJSONString();
                    } catch (Exception e) {
                        System.out.println("what");
                        return "";
                    }
                }
                System.out.println("whaat");
                return "";
            }
        };
        return map;
    }
}
