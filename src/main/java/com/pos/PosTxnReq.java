package com.pos;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;

public class PosTxnReq {
    public final int txnID;
    public final int MCC;

    public PosTxnReq(int txnID, int MCC) {
        this.txnID = txnID;
        this.MCC = MCC;
    }

    public String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public static PosTxnReq fromJSONString(String some) {
        Gson gson = new Gson();
        return gson.fromJson(some, PosTxnReq.class);
    }

    public static MapFunction<String, String> mapPosTransactions() {
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
}
