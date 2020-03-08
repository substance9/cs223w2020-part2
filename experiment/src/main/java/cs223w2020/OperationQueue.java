package main.java.cs223w2020;


import main.java.cs223w2020.model.Operation;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class OperationQueue {
    private BlockingQueue<Operation> oQueue;

    public OperationQueue() {
        oQueue = new LinkedBlockingQueue<>();
    }

    public void put(Operation op) {
        try {
            oQueue.put(op);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public Operation take() {
        //blocking
        Operation ret = null;
        try {
            ret = oQueue.take();
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public Operation poll() {
        //non-blocking
        Operation ret = null;
        try {
            ret = oQueue.poll();
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public int getSize(){
        return oQueue.size();
    }

}