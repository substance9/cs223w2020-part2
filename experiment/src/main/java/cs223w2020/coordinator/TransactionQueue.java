package cs223w2020.coordinator;


import cs223w2020.model.Transaction;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TransactionQueue {
    private BlockingQueue<Transaction> txQueue;

    public TransactionQueue() {
        txQueue = new LinkedBlockingQueue<>();
    }

    public void put(Transaction tx) {
        try {
            txQueue.put(tx);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public Transaction take() {
        //blocking
        Transaction ret = null;
        try {
            ret = txQueue.take();
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public Transaction poll() {
        //non-blocking
        Transaction ret = null;
        try {
            ret = txQueue.poll();
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public int getSize(){
        return txQueue.size();
    }

}