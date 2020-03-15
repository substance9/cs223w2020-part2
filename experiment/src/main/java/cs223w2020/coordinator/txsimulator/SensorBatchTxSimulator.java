package cs223w2020.coordinator.txsimulator;

import java.sql.Timestamp;
import java.util.HashMap; 
import java.util.LinkedList;

import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;
import cs223w2020.coordinator.OperationQueue;
import cs223w2020.coordinator.TransactionQueue;

public class SensorBatchTxSimulator extends TxSimulator 
{
    public SensorBatchTxSimulator(OperationQueue opQueue, TransactionQueue txQueue, int maxTxCount){
        super(opQueue, txQueue, maxTxCount);
        opListMap = new HashMap<String, LinkedList<Operation>>();
    }

    private OperationQueue opQueue;

    private int BATCHSIZE = 5;

    private HashMap<String, LinkedList<Operation>> opListMap;

    @Override
    public void processNewOperation(Operation op){
        if(opListMap.containsKey(op.sensorId)){
            LinkedList<Operation> opList = opListMap.get(op.sensorId);
            opList.add(op);
            if (opList.size() >= BATCHSIZE){
                formAndSendTransaction(opList);
                opListMap.remove(op.sensorId);
            }
        }
        else{
            LinkedList<Operation> opList = new LinkedList<Operation>();
            opList.add(op);
            opListMap.put(op.sensorId,opList);
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