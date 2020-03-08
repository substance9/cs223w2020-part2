package main.java.cs223w2020.txsimulator;

import java.sql.Timestamp;
import java.util.Date;

import main.java.cs223w2020.OperationQueue;
import main.java.cs223w2020.TransactionQueue;
import main.java.cs223w2020.model.Operation;
import main.java.cs223w2020.model.Transaction;

public class TxSimulator implements Runnable 
{ 
    private OperationQueue opQueue;
    private TransactionQueue txQueue;

    public TxSimulator(OperationQueue opQueue, TransactionQueue txQueue){
        this.opQueue = opQueue;
        this.txQueue = txQueue;
    }

    public void run() 
    {
        Operation op;
        Date date;
        long time;
        int opCount = 0;
        while(true){
            op = opQueue.poll();
            if(!(op == null)){
                //System.out.println(op);
                if(op.operationStr.equals("END")){
                    Transaction tx = new Transaction();
                    sendTransaction(tx);
                    return;
                }
                else{
                    processNewOperation(op);
                }
            }
            date = new Date();
            time = date.getTime();
            Timestamp ts = new Timestamp(time);
            processNowTimeTick(ts);
        }
    }

    public void sendTransaction(Transaction tx){
        tx.setConstructTimeToNow();
        txQueue.put(tx);
    }

    //needs to be overriden in child class
    public void processNewOperation(Operation op){

    }

    //needs to be overriden in child class
    public void processNowTimeTick(Timestamp ts){

    }
} 