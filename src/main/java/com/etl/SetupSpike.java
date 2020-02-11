package com.etl;

import com.inmem.Transaction;
import com.pos.PosTxnReq;

import java.util.ArrayList;
import java.util.List;

public class SetupSpike {
    public static void main(String[] args) {
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
    }
}
