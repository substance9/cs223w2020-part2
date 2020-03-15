package cs223w2020.coordinator.txsimulator;

import java.sql.Timestamp;

import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;
import cs223w2020.coordinator.OperationQueue;
import cs223w2020.coordinator.TransactionQueue;

public class SingleTxSimulator extends TxSimulator 
{
    private OperationQueue opQueue;

    public SingleTxSimulator(OperationQueue opQueue, TransactionQueue txQueue, int maxTxCount){
        super(opQueue, txQueue, maxTxCount);
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