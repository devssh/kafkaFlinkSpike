package com.etl;

import com.google.gson.Gson;
import com.inmem.Transaction;
import com.pos.PosTxnReq;

public class PosTxnExpanded {
    final int txnID;
    final float amt;
    final String account;
    final int MCC;

    public PosTxnExpanded(Transaction inmem, PosTxnReq req) {
        if (inmem.txnID != req.txnID) {
            throw new NullPointerException("txnID do not match in PosTXN and TXN");
        }
        txnID=inmem.txnID;
        amt=inmem.amt;
        account=inmem.account;
        MCC=req.MCC;
    }

    public String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public PosTxnExpanded fromJSONString(String some) {
        Gson gson = new Gson();
        return gson.fromJson(some, PosTxnExpanded.class);
    }
}
