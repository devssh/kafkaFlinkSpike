package com.inmem;

import com.etl.PosTxnExpanded;
import com.google.gson.Gson;
import com.pos.PosTxnReq;
import org.apache.flink.api.common.functions.MapFunction;

public class Transaction {
    public final int txnID;
    public final float amt;
    public final String account;

    public Transaction(int txnID, float amt, String account) {
        this.txnID = txnID;
        this.amt = amt;
        this.account = account;
    }


    public String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public PosTxnExpanded expand(PosTxnReq req) {
        return new PosTxnExpanded(this, req);
    }

    public static Transaction fromJSONString(String some) {
        Gson gson = new Gson();
        return gson.fromJson(some, Transaction.class);
    }

    public static MapFunction<String, String> mapTransactions() {
        MapFunction<String, String> map = new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value != null || value.trim().length() > 0) {
                    try {
                        return fromJSONString(value).toJSONString();
                    } catch (Exception e) {
                        return "";
                    }
                }
                return "";
            }
        };
        return map;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "txnID=" + txnID +
                ", amt=" + amt +
                ", account='" + account + '\'' +
                '}';
    }
}
