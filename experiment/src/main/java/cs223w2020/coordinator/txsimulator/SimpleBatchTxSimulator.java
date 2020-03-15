package cs223w2020.coordinator.txsimulator;

import java.sql.Timestamp;
import java.util.HashMap; 
import java.util.LinkedList;

import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;
import cs223w2020.coordinator.OperationQueue;
import cs223w2020.coordinator.TransactionQueue;

public class SimpleBatchTxSimulator extends TxSimulator 
{
    public LinkedList<Operation> opList;

    public SimpleBatchTxSimulator(OperationQueue opQueue, TransactionQueue txQueue, int maxTxCount){
        super(opQueue, txQueue, maxTxCount);
        opList = new LinkedList<Operation>();
    }

    private int BATCHSIZE = 20;

    @Override
    public void processNewOperation(Operation op){
        if (opList.size() < BATCHSIZE){
            opList.add(op);
        }
        else{
            formAndSendTransaction(opList);
            opList = new LinkedList<Operation>();
        }
    }

    @Override
    public void processNowTimeTick(Timestamp ts){
        return;
    }

    public void formAndSendTransaction(LinkedList<Operation> opList){
        Transaction tx = new Transaction();
        Operation opl = opList.pollFirst();
        while(opl!=null){
            tx.appendOperation(opl);
            opl = opList.pollFirst();
        }
        //System.out.println("OP Queue size: " + tx.operations.size());
        sendTransaction(tx);
    }
} 