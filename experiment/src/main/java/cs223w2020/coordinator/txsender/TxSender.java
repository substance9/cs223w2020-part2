package cs223w2020.coordinator.txsender;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService; 
import java.util.concurrent.Executors; 
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.TimeUnit;

import cs223w2020.coordinator.OperationQueue;
import cs223w2020.coordinator.TransactionQueue;
import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;

public class TxSender implements Runnable 
{ 
    private TransactionQueue txQueue;
    private int numAgents;
    private int agentsPortsStartAt;
    private ArrayList<AgentClient> agentClientList;
    private ArrayList<Thread> agentClientThreadList;

    public TxSender(TransactionQueue txQueue, int numAgents, int agentsPortsStartAt, String resOutputDir){
        this.txQueue = txQueue;
        this.numAgents=numAgents;
        this.agentsPortsStartAt = agentsPortsStartAt;

        agentClientList = new ArrayList<AgentClient>();
        agentClientThreadList = new ArrayList<Thread>();

        for(int i = 0; i < numAgents; i++){
            AgentClient aclient = new AgentClient(agentsPortsStartAt + i);
            agentClientList.add(aclient);
        }
    }

    public int connectToAgents(){
        for(int i = 0; i < numAgents; i++){
            AgentClient aclient = agentClientList.get(i);
            try {
                aclient.startConnection();
            } catch (IOException e) {
                e.printStackTrace();
                return -1;
            }
        }
        return 0;
    }

    public int getCorrespondAgentId(Operation op){
        int opHashCode = op.sensorId.hashCode() + op.timestamp.hashCode();
        if (opHashCode < 0){
            opHashCode = 0 - opHashCode;
        }
        return (opHashCode % numAgents);
    }

    public void processTxIn2PC(Transaction tx){
        System.out.println(tx.toString());
        for (int i = 0; i < tx.operations.size(); i ++){
            Operation op = tx.operations.get(i);
            int agentId = getCorrespondAgentId(op);
            AgentClient aclient = agentClientList.get(agentId);
            aclient.addSqlToSendQueue(op.sqlStr);
        }
    }

    public void run() 
    {
        for(int i = 0; i < numAgents; i++){
            AgentClient aclient = agentClientList.get(i);
            Thread acThread = new Thread(aclient);
            agentClientThreadList.add(acThread);
            acThread.start();
        }

        Transaction tx = null;
        while(true){
            //1. get the transaction from queue
            tx = txQueue.take();
            if (tx.operations.size()>0){
                processTxIn2PC(tx);
            }
            else{
                //end mark transaction
                
                
                return;
            }
        }
    }
} 