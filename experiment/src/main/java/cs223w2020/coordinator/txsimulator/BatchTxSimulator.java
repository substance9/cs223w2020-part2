package cs223w2020.coordinator.txsimulator;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap; 
import java.util.LinkedList;

import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;
import cs223w2020.coordinator.OperationQueue;
import cs223w2020.coordinator.TransactionQueue;

public class BatchTxSimulator extends TxSimulator 
{ 

    class SensorTime{
        public long batchStartTime;
        public String sensorId;
    }

    private OperationQueue opQueue;
    private String lastSensorId;
    private HashMap<String, LinkedList<Operation>> opListMap;
    private LinkedList<SensorTime> sensorTimeList;
    private long timeWindow;

    public BatchTxSimulator(OperationQueue opQueue, TransactionQueue txQueue){
        super(opQueue, txQueue);
        opListMap = new HashMap<String, LinkedList<Operation>>();
        sensorTimeList = new LinkedList<SensorTime>();
        timeWindow = 60*1000; //ms
    }

    @Override
    public void processNewOperation(Operation op){
        if(sensorTimeList.size()>0){
            while(sensorTimeList.getFirst().batchStartTime + timeWindow <= op.timestamp.getTime()){
                SensorTime sTime = sensorTimeList.pollFirst();//get and remove
                LinkedList<Operation> opList = opListMap.get(sTime.sensorId);
                opListMap.remove(sTime.sensorId);
                Transaction tx = new Transaction();
                Operation opl = opList.pollFirst();
                while(opl!=null){
                    tx.appendOperation(opl);
                    opl = opList.pollFirst();
                }
                if(tx.operations.size()>0){
                    sendTransaction(tx);
                }
                
                if (sensorTimeList.size()==0){
                    break;
                }
            }
        }

        if (op.operationStr.equals("SELECT")){
            Transaction queryTx = new Transaction();
            queryTx.appendOperation(op);
            sendTransaction(queryTx);
        }
        else{
            if(opListMap.containsKey(op.sensorId)){
                opListMap.get(op.sensorId).add(op);
            }
            else{
                LinkedList<Operation> opList = new LinkedList<Operation>();
                opList.add(op);
                opListMap.put(op.sensorId,opList);
                SensorTime sTime = new SensorTime();
                sTime.sensorId = op.sensorId;
                sTime.batchStartTime = op.timestamp.getTime();
                sensorTimeList.add(sTime);
            }
        }
    }

    @Override
    public void processNowTimeTick(Timestamp ts){
        
        return;
    }

    
} 