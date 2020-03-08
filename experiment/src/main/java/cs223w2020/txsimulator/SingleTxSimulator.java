package main.java.cs223w2020.txsimulator;

import java.sql.Timestamp;

import main.java.cs223w2020.model.Operation;
import main.java.cs223w2020.model.Transaction;
import main.java.cs223w2020.OperationQueue;
import main.java.cs223w2020.TransactionQueue;

public class SingleTxSimulator extends TxSimulator 
{
    private OperationQueue opQueue;

    public SingleTxSimulator(OperationQueue opQueue, TransactionQueue txQueue){
        super(opQueue, txQueue);
    }

    @Override
    public void processNewOperation(Operation op){
        Transaction tx = new Transaction();
        tx.appendOperation(op);
        //System.out.println("OP Queue size: " + tx.operations.size());
        sendTransaction(tx);
    }

    @Override
    public void processNowTimeTick(Timestamp ts){
        return;
    }
} 